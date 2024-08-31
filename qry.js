const mysql = require('mysql2')
const { copyObj, parseJson, parseFlatted, stringifyFlatted, sNull, unsNull, prepPackrPackData } = require('./helper')
const { Packr } = require('msgpackr')
const refuncs = require('./redis-util-functions/redis-util-functions');

let db = null

let _default_options = {
	rkey: null,
	rpath: null,
	r_packr: null,
	rmulti: null,
	rjson: null,
	rfield: null,
	rjson_flatted: false,
	rnoread: false,
	rcb: null,
	rexpire: 0,
	rawait: false,
	rdel: null,
	rkeepttl: false,
	more_data: null,
	silent_error: false,
}

const setOptions = function(options, defaults) {
	const out_options = Object.assign({}, defaults)
	if (!options || typeof options !== 'object') return out_options

	for (let k in out_options) {
		if (!out_options.hasOwnProperty(k) || options[k] === out_options[k]) continue
		if (typeof out_options[k] === 'boolean') {
			out_options[k] = Boolean(options[k])
			continue
		} else
		if (['rpath', 'rmulti', 'rjson', 'rcb', 'more_data', ].indexOf(k) !== -1) {
			out_options[k] = options[k]
			continue
		} else
		if (['rkey', 'rfield', ].indexOf(k) !== -1) {
			if ((typeof options[k] === 'string' && options[k].length) || typeof options[k] === 'function') out_options[k] = options[k]			
		} else 
		if (k === 'rexpire') {
			const _rexpire = Number(options.rexpire)
			out_options.rexpire = Number.isInteger( _rexpire) && _rexpire > 0 ? _rexpire : 0			
		} else 
		if (k === 'r_packr') {
			if(options.r_packr && options.r_packs instanceof Packr) out_options.r_packr = options.r_packr			
		} else 
		if (k === 'rdel') {
			if (options.rdel && (typeof options.rdel === 'string' || (Array.isArray(options.rdel) && options.rdel.length))) out_options.rdel = options.rdel
		}
	}
	return out_options
}


/**
 * Execute a query on the database and/or Redis.
 * @param {string} query - the SQL query to execute
 * @param {Array} query_args - the arguments for the query
 * @param {function} cb - the callback function to call with the result
 * @param {Object} opts - options object
 * @param {Object} [opts.db] - the database connection to use
 * @param {boolean} [opts.redis] - whether to use Redis
 * @param {string} [opts.rkey] - the Redis key to use
 * @param {string} [opts.rpath] - the Redis path to use
 * @param {string} [opts.rfield] - the Redis field to use
 * @param {boolean} [opts.rmulti] - whether to use Redis multi commands
 * @param {boolean} [opts.rjson] - whether to store the value in Redis as JSON
 * @param {boolean} [opts.rjson_flatted] - whether to store the value in Redis as flatted JSON
 * @param {Packr} [opts.r_packr] - the packr instance to use for packing/unpacking values
 * @param {boolean} [opts.rnoread] - whether to not read the value from Redis
 * @param {boolean} [opts.rawait] - whether to wait for the Redis write to complete before resolving the Promise
 * @param {boolean} [opts.rkeepttl] - whether to keep the Redis TTL
 * @param {number} [opts.rexpire] - the Redis TTL to set
 * @param {string|string[]} [opts.rdel] - the Redis key(s) to delete
 * @param {function} [opts.rcb] - the callback function to call with the value from Redis
 * @param {boolean} [opts.silent_error] - whether to not throw an error if the database query fails
 * @returns {Promise} a Promise resolved with the result of the query
 */
const qry = async (query, query_args, cb, opts) => {
		let _db = null
		if (opts.db) {
			_db = opts.db			
			if (!db) db = _db
		} else {
			if (!db) return new Promise((resolve, reject) => reject('DB Connection Required'))
			_db = db
		}

		if (opts.redis) {
			refuncs.setClient(opts.redis)
		} else 	if (!refuncs.redisClient) {
			return new Promise((resolve, reject) => reject('Redis Connection Required'))	
		}
			

		let redis_result = null
		let _args
		let _cb
		let redis_methods

		if (opts === undefined || typeof opts !== 'object') {
			if (typeof cb === 'object') {
				opts = copyObj.copy(cb)
			} else {
				opts = {}
			}
		}
		opts = setOptions(opts, _default_options)

		if (opts.rkey) {
			if (opts.r_packr) {
				if (opts.rmulti) {
					if (opts.rmulti === 'list') {
						redis_methods = ['lrangeBuffer', 'rpush']
					} else {
						redis_methods = ['smembersBuffer', 'sadd']
					}
				} else {
					redis_methods = ['getBuffer', 'set']
				}
			} else if (opts.rpath) {
				if (typeof opts.rpath !== 'string') opts.rpath = '$'
				opts.rjson = opts.rfield = opts.rmulti = false
				redis_methods = ['JSON.GET', 'JSON.SET']
			} else if (opts.rfield) {
				redis_methods = ['hget', 'hset']
			} else if (opts.rmulti) {
				redis_methods = ['hgetall', 'hset']
			} else {
				redis_methods = ['get', 'set']
			}
		}

		const json_parse_func = opts.rjson_flatted ? parseFlatted : parseJson
		const json_stringify_func = opts.rjson_flatted ? stringifyFlatted : JSON.stringify

		if (redis_methods && !opts.rnoread && typeof opts.rkey !== 'function') {
			try {
				redis_result = await new Promise((resolve, reject) => {
					_args = [redis_methods[0], opts.rkey, (r_error, r_obj) => {
						if (r_error || r_obj === undefined) return reject(false)
						if (opts.r_packr) {
							if (!r_obj || (Array.isArray(r_obj) && !r_obj.length)) return resolve(null)
							if (opts.rmulti) {
								r_obj = r_obj.map(_r => opts.r_packr.unpack(_r))
							} else {
								r_obj = opts.r_packr.unpack(r_obj)
							}
						} else if (opts.rpath && r_obj !== null) {
							r_obj = json_parse_func(r_obj, null)
							if (Array.isArray(r_obj) && r_obj.length === 1) r_obj = r_obj[0]
						} else if (opts.rjson) {
							r_obj = json_parse_func(r_obj, opts.rjson)
						}	
						if (opts.rcb && typeof opts.rcb === 'function') r_obj = opts.rcb(r_obj)
						return resolve(r_obj)
					}]
					if (['JSON.GET', 'hget'].indexOf(redis_methods[0]) !== -1) {
						_args.splice(2, 0, opts[redis_methods[0] === 'hget' ? 'rfield' : 'rpath'])
					} else if (redis_methods[0] === 'lrangeBuffer') {
						_args.splice(2, 0, 0, -1)					
					}
					refuncs.r(..._args)
				})
				if (redis_result && redis_methods[0] === 'hgetall') redis_result = !Object.keys(redis_result).length ? null : unsNull(redis_result)
			} catch (_exc) {
				redis_result = null
			}
		}

		if (typeof cb !== 'function') {
			_cb = (result) => result
		} else {
			_cb = cb
		}

		let f = (resolve, reject) => {
			if (redis_result !== null || !query) return resolve(redis_result)
			_db.query(query, query_args || [], async (db_err, db_result) => {
				if (db_err) return (opts.silent_error ? resolve(null) : reject(db_err))

				let cb_result = _cb(db_result, reject, opts.more_data)
				if (cb_result instanceof Promise) cb_result = await cb_result
				if (!opts.rawait) resolve(cb_result)

				if (opts.rdel) {
					if (!Array.isArray(opts.rdel)) opts.rdel = [opts.rdel]
					let _commands = []
					opts.rdel.forEach((_rdel) => {
						_rdel = _rdel.split('.')
						if (!_rdel.length) {
							if (opts.rawait) resolve(cb_result)
							return
						}
						if (_rdel.length === 1) {
							_commands.push(['del', _rdel[0]])
						} else {
							_commands.push(['hdel', ..._rdel])
						}
					})
					if (_commands.length) _commands.length > 1 ? refuncs.rpipemulti(_commands) : refuncs.r(..._commands[0])					
				}

				if (redis_methods && cb_result !== null) {
					if (typeof opts.rkey === 'function') opts.rkey = opts.rkey(cb_result)
					if (opts.rfield && typeof opts.rfield === 'function') opts.rfield = opts.rfield(cb_result)
					if (!opts.rpath && !opts.rjson && !opts.r_packr && typeof cb_result === 'object' && (Array.isArray(cb_result) || redis_methods[0] !== 'hgetall')) opts.rjson = true
					_args = [redis_methods[1], opts.rkey,]
					
					if (opts.r_packr) {
						if (opts.rmulti) {
							let _cb_result = Array.isArray(cb_result) ? cb_result : [cb_result]
							_args.push(..._cb_result.map(_r => opts.r_packr.pack(prepPackrPackData(opts.r_packr, _r))))
						} else {							
							_args.push(opts.r_packr.pack(prepPackrPackData(opts.r_packr, cb_result)))
						}
					} else {
						_args.push(opts.rjson ? json_stringify_func(cb_result) : (redis_methods[0] === 'hgetall' ? sNull(cb_result) : cb_result))
					}
					
					if (opts.rpath || opts.rfield) {
						_args.splice(2, 0, (opts.rpath || opts.rfield))
					} else if (redis_methods[1] === 'set' && opts.rexpire > 0) {
						_args.splice(3, 0, 'EX', opts.rexpire)
					}
					if (opts.rawait) _args.push(() => resolve(cb_result))
					let _commands = []
					opts.rkeepttl ? refuncs.r(..._args) : _commands.push(_args)

					if (opts.rexpire > 0 && redis_methods[1] !== 'set') {
						let _set_expire = true
						if (opts.rkeepttl) {
							let _ttl = await refuncs.rr('ttl', opts.rkey)
							if (_ttl > 0) _set_expire = false
						}
						if (_set_expire) opts.rkeepttl ? refuncs.r('expire', opts.rkey, opts.rexpire) :  _commands.push(['expire', opts.rkey, opts.rexpire])
					}
					if (_commands.length) _commands.length > 1 ? refuncs.rpipemulti(_commands) : refuncs.r(..._commands[0])
				}
			})
		}
		return new Promise(f)
}


module.exports = qry
