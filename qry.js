const mysql = require('mysql2')
const { copyObj, parseJson, parseFlatted, stringifyFlatted, sNull, unsNull, prepPackrPackData } = require('./helper')
const RedisUtilFunctions = require('./redis-util-functions/redis-util-functions');

class DbConnection {

	constructor() {
		this.redis = RedisUtilFunctions
		this.pool = null
		this.pool_archive = null
		this.db = null
		this.redisClient = null		
		this.scServer = null
		this.videoFootages = []
		this.intercom = null
		this.activeAutomations = []
		this.redisHprefix = process.env.REDIS_HPREFIX ? process.env.REDIS_HPREFIX : false
		this.STOP_FP = false
	}

	setPoolEvents(opts) {
		let pool_key = opts && opts.pool ? opts.pool : 'pool'
		this[pool_key].on('connection', (connection) => {
			logger.CLOG('DB Connection established.'+(pool_key !== 'pool' ? ' ('+pool_key+')' : ''), 'force')

			connection.on('error', async (err) => {
				logger.CLOGJ('DB Connection error'+(pool_key !== 'pool' ? ' ('+pool_key+')' : ''), err, 'force')
				logger.ERROR.error(logger.lineNumber(new Error()) + ' | ' + err)
				//this.open()
				await this.createPool(false, opts)
			})
			connection.on('close', function (err) {
				console.error(new Date(), 'DB Connection close'+(pool_key !== 'pool' ? ' ('+pool_key+')' : ''), err)
			});
		})		
	}

	async createPool(dont_end, opts) {
		let pool_key = 'pool'
		if(opts) {
			if(opts.pool) pool_key = opts.pool
		} else {
			opts = process.env
		}
		if (this[pool_key] && !dont_end) {
			await new Promise((resolve, reject) => {
				this[pool_key].end((err) => {
					setTimeout(() => {
						this.createPool(true, opts)
						return resolve(1)
					}, 50);
				})
			})
		} else {
			this[pool_key] = mysql.createPool({
				host: opts.DB_HOSTNAME,
				user: opts.DB_USERNAME,
				password: opts.DB_PASSWORD,
				database: opts.DB_NAME,

				connectTimeout: 20000,
				queueLimit: 0,
				//connectionLimit: 10000,
				waitForConnections: true,				
			})
		}
	}


	async qry(query, query_args, cb, opts) {
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
					this.r(..._args)
				})
				if (redis_result && redis_methods[0] === 'hgetall') redis_result = !Object.keys(redis_result).length ? null : unsNull(redis_result)
			} catch (_exc) {
				logger.CLOGJ(logger.lineNumber(new Error(), 2), _exc, 'force')
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
			//if(Array.isArray(query_args)) query_args = ar2null(query_args)
			let _db = opts.db ? opts.db : this.pool
			_db.query(query, query_args || [], async (db_err, db_result) => {
				if (db_err) {
					logger.CLOGJ(logger.lineNumber(new Error(), 2) + ' | ' + db_err + ' SQL: ' + db_err.sql, 'force')
					return (opts.silent_error ? resolve(null) : reject(db_err))
				}

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
					if (_commands.length) _commands.length > 1 ? this.rpipemulti(_commands) : this.r(..._commands[0])					
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

					if (!opts.rexpire) opts.rexpire = parseInt(process.env.REDIS_EXPIRE)
					if (opts.rpath || opts.rfield) {
						_args.splice(2, 0, (opts.rpath || opts.rfield))
					} else if (redis_methods[1] === 'set' && opts.rexpire > 0) {
						_args.splice(3, 0, 'EX', opts.rexpire)
					}
					if (opts.rawait) _args.push(() => resolve(cb_result))
					let _commands = []
					opts.rkeepttl ? this.r(..._args) : _commands.push(_args)

					if (opts.rexpire > 0 && redis_methods[1] !== 'set') {
						let _set_expire = true
						if (opts.rkeepttl) {
							let _ttl = await this.rr('ttl', opts.rkey)
							if (_ttl > 0) _set_expire = false
						}
						if (_set_expire) opts.rkeepttl ? this.r('expire', opts.rkey, opts.rexpire) :  _commands.push(['expire', opts.rkey, opts.rexpire])
					}
					if (_commands.length) _commands.length > 1 ? this.rpipemulti(_commands) : this.r(..._commands[0])
				}
			})
		}
		return new Promise(f)
	}



}

module.exports = new DbConnection()
