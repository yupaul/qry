const moment = require('moment-timezone')
const mysql = require('mysql2')
const logger = require('./config/logger');
const Intercom = require('./config/intercom')
const Redis = require('ioredis')
const { copyObj, parseJson, parseFlatted, stringifyFlatted, sNull, unsNull, promisify, prepPackrPackData } = require('./helper')
const { default: Redlock } = require('redlock')

const RedisUtilFunctions = require('./redis-util-functions/redis-util-functions');

const rlock_default_retry_count = 100

class DbConnection {

	constructor() {
		this.pool = null
		this.pool_archive = null
		this.db = null
		this.redisClient = null
		this.redlock = null
		this.scServer = null
		this.videoFootages = []
		this.intercom = null
		this.activeAutomations = []
		this.redisHprefix = process.env.REDIS_HPREFIX ? process.env.REDIS_HPREFIX : false
		this.STOP_FP = false
	}

	stopFp() {
		this.STOP_FP = true
	}

	rlock(keys, time, retryCount) {
		if(!Array.isArray(keys)) keys = [keys]
		if(retryCount === undefined || retryCount === null) retryCount = rlock_default_retry_count
		keys = keys.map((_k) => `${ _k }_redlock`)
		return this.redlock.acquire(keys, time, {retryCount: retryCount})
	}

	rpipemaybe(commands) {		
		return (
			commands.length === 1 ?
			this.rr(...commands[0]) :
			this.rpipemulti(commands)			
		)		
	}

	rpipemulti(commands, arg2, cb) { //arg2 - 't' for transaction, function for callback
//let dt = Date.now() //-tmp	
		let type = 'pipeline'
		if (arg2) {
			if (typeof arg2 === 'function') {
				cb = arg2
			} else {
				if (arg2 === 't') type = 'multi'
				if (cb && typeof cb !== 'function') cb = null
			}
		}

		let rpipe = this.redisClient[type]()
		for (let i = 0; i < commands.length; ++i) {
			if (commands[i].length > 1) commands[i][1] = this._rpfx(commands[i][1])
			commands[i].push(rpipe)
			this._redis_call(...commands[i])
		}

		let ret = cb ? rpipe.exec((err, results) => cb(err, results)) : rpipe.exec()
		return ret
	}

	async rpipemulti2array(commands) {
		let rpipe = this.redisClient.pipeline()
		for (let i = 0; i < commands.length; ++i) {
			if (commands[i].length > 1) commands[i][1] = this._rpfx(commands[i][1])
			commands[i].push(rpipe)
			this._redis_call(...commands[i])
		}

		let ret = await rpipe.exec()
		if(!Array.isArray(ret) || !ret.length) return promisify(ret)
		return promisify(ret.map(r => r[1]))
	}

	_rexec(rest_args) {
		if (rest_args && rest_args[rest_args.length - 1] && rest_args[rest_args.length - 1].exec) return rest_args.pop()
		return this.redisClient
	}

	_redis_call(redis_method, hkey, ...rest_args) {
		if (redis_method.indexOf('.') !== -1) {
			rest_args.unshift(hkey)
			return this._rraw(redis_method, ...rest_args)
		} else {
			if(rest_args.length > 0 && hkey.substring(0, 1) === '{' && (redis_method === 'del' || (redis_method.substring(0, 1) === 'z' && redis_method.indexOf('store') !== -1))) rest_args = rest_args.map(rest_arg => (typeof rest_arg !== 'string' || rest_arg.substring(0, 1) !== '{') ? rest_arg : this._rpfx(rest_arg))
			
			const execer = this._rexec(rest_args)
			return execer[redis_method](hkey, ...rest_args)
		}
	}

	_rraw(redis_method, ...rest_args) {
		const execer = this._rexec(rest_args)
		if (redis_method.indexOf('JSON.') === 0 && rest_args.length >= 3 && (rest_args[2] !== null || redis_method === 'JSON.SET' || redis_method === 'JSON.ARRINSERT' || redis_method === 'JSON.ARRAPPEND') && typeof rest_args[2] !== 'function') rest_args[2] = JSON.stringify(rest_args[2])
		if(redis_method === 'JSON.ARRINSERT' && rest_args.length >= 4) [rest_args[3], rest_args[2]] = [rest_args[2], rest_args[3]]
		return execer.call(redis_method, ...rest_args)
	}

	r(redis_method, hkey, ...rest_args) {
		hkey = this._rpfx(hkey)
		if (!rest_args.length || typeof rest_args[rest_args.length - 1] !== 'function') rest_args.push(function() {})
		return this._redis_call(redis_method, hkey, ...rest_args)
	}

	rr(redis_method, hkey, ...rest_args) {
		hkey = this._rpfx(hkey)
		let cb = rest_args.length > 0 && typeof rest_args[rest_args.length - 1] === 'function' ? rest_args.pop() : null
		if (!cb) return this._redis_call(redis_method, hkey, ...rest_args)

		return new Promise(async (resolve, reject) => {
			let f = (err, result) => {
				if (err) {
					logger.ERROR.error(logger.lineNumber(new Error(), 2) + ' | ' + err)
					return reject(err)
				}
				return resolve(cb(result))
			}
			rest_args.push(f)
			this._redis_call(redis_method, hkey, ...rest_args)
		})
	}

	rhmget(hkey, keys, to_num, cb) {
		if(cb && typeof cb !== 'function') cb = null
		return this.rr('hmget', hkey, keys, (res) => {
			let out = {}
			for(let i = 0; i < keys.length; ++i) {
				out[keys[i]] = cb ? cb(res[i]) : (to_num ? res[i] - 0 : res[i])
			}
			return out
		})
	}
	
	rrename(from, to) {
		return this._redis_call('rename', this._rpfx(from), this._rpfx(to))
	}

	async rdel_from_set(keys_set, is_sorted, ptn) {
		let _keys
		const command = is_sorted ? 'zpopmin' : 'spop'
		while(true) {			
			_keys = await this.rr(command, keys_set, 500)
			if(!_keys || !Array.isArray(_keys) || !_keys.length) break
			if(is_sorted) _keys = _keys.filter((x, y) => !(y % 2))
			await this.rpipemulti(_keys.map(_key => ['del', ptn ? ptn.replace('*', _key) : _key]))
		}
	}

	async rdel(ptns) {	
		if(!Array.isArray(ptns)) ptns = [ptns]
		let commands = []
		for(let ptn of ptns) {		
			if (!ptn) continue
			if (ptn.indexOf('*') === -1) {
				ptn = ptn.split('.')
				commands.push(ptn.length === 1 ? ['del', ptn[0]] : ['hdel', ...ptn])
			} else {
				await this.rscan(ptn, _keys => {
					commands = commands.concat(_keys.map(_key => ['del', _key]))
				}, null, {cb_all : true, count: 1000})		
			}			
		}
		if(commands.length) await this.rpipemaybe(commands)
		return promisify(true)
	}

	async rscan(ptn, cb, hkey, opts) {
		//opts: return, return_cursor, one, cursor, cb_all, count
		if(!opts) opts = {}
		let params = []
		if(hkey) {
			hkey = this._rpfx(hkey)
			params.push(hkey)
		} else if (ptn) {
			ptn = this._rpfx(ptn)
		}
		let cb_ret
		let keys = []	

		let i = opts.cursor ? opts.cursor - 0 : 0
		if (typeof cb !== 'function') cb = false
		const func = hkey ? 'hscan' : 'scan'
		const num_params = func === 'scan' ? 1 : 2
		params.push(i)
		if(ptn) params.push('MATCH', ptn)
		if(opts.count) params.push('COUNT', opts.count)

		while (true) {
			let result = await new Promise((resolve) => this.redisClient[func](...params, (_err, _result) => {
				resolve(_result)
			}))

			if (!Array.isArray(result) || !result.length) break
			//if()
			i = parseInt(result[0])
			params[func === 'scan' ? 0 : 1] = i
			if (result.length > 1 && Array.isArray(result[1]) && result[1].length) {
				if (cb) {
					if(opts.cb_all) {
						cb_ret = cb(result[1])
						if(cb_ret instanceof Promise) await cb_ret
					} else {					
						while(result[1].length) {
							let params_cb = result[1].splice(-num_params)
							cb_ret = cb(...params_cb)
							if(cb_ret instanceof Promise) await cb_ret
						}
					}
				} else if (opts.return) {
					keys.splice(keys.length, 0, ...result[1])
				}
			}
			if (!i || opts.one) break
		}
		return (opts.return_cursor ? [i, keys] : keys)
	}

	open(dont_flush_redis) {
		if (parseInt(process.env.REDIS_CLUSTER)) {
			this.redisClient = new Redis.Cluster([process.env.REDIS_CONNECTION])
		} else {
			this.redisClient = new Redis(process.env.REDIS_CONNECTION)
		}

		this.redlock = new Redlock(
			[this.redisClient,],
			{
			  driftFactor: 0.01,
			  retryCount: rlock_default_retry_count,
			  retryDelay: 200, 
			  retryJitter: 100,
			  automaticExtensionThreshold: 300,
			}
		)
		this.createPool().then(() => this.setPoolEvents())
	}

	openArchive() {
		if(!this.pool_archive) {
			const opts = {
				pool: 'pool_archive',
			}
			const fields = ['HOSTNAME', 'USERNAME', 'PASSWORD', 'NAME']
			for(let field of fields) {
				opts['DB_' + field] = process.env['DB_' + field + '_ARCHIVE'] || process.env['DB_' + field]
			}
			this.createPool(false, opts)
			this.setPoolEvents(opts)	
		}		
		return this.pool_archive
	}

	closeArchive() {
		if(this.pool_archive) this.pool_archive.end(() => {
			this.pool_archive = null
		})		
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
				timezone: moment.tz(process.env.TIMEZONE).format('Z'),
			})
		}
	}

	async rjget(hkey, path, callback, ...rest_args) {
		// if key doesn't exists return null
		// if key exists, but path doesn't - returns []
		path = this._rjpath(path)
		hkey = this._rpfx(hkey)
		let res = await this._rraw('JSON.GET', hkey, path)
		let opts = {
			rerun: false,
			keep_array: false,
			empty_array_null: false,
		}		
		
		Object.keys(opts).forEach((_opt) => {			
			let _indx = rest_args.indexOf(_opt)
			if(_indx !== -1) {
				opts[_opt] = true
				rest_args.splice(_indx, 1)				
			}			
		})
		
		if (res !== null) {
			if (typeof res === 'string') res = parseJson(res, res)
			if (opts.empty_array_null && Array.isArray(res) && !res.length) res = null
			if (res !== null) {
				if (!opts.keep_array && Array.isArray(res) && res.length === 1) res = res[0]
				if (callback && typeof callback === 'function') {
					res = callback(res)
					if (res instanceof Promise) res = await res
				}
			}
		}
		if (res !== null || !rest_args.length) return promisify(res)

		let ret
		const _type = typeof rest_args[0]
		if (_type === 'string') {
			ret = this.qry(...rest_args)
		} else if (_type === 'function') {
			ret = rest_args[0](hkey, path)
		} else {
			return promisify(null)
		}
		if (opts.rerun) {
			await ret
			return this.rjget(hkey, path, callback)
		} else {
			return ret
		}
	}

	async rjset(hkey, path, data, ...rest_args) {
		//hkey path [db_query] [db_query_params, Array or function] [callback]
		const db_args = rest_args.length && typeof rest_args[0] === 'string' ? rest_args.splice(0, 2) : false
		path = this._rjpath(path)
		if (db_args) {
			await this.rr('JSON.SET', hkey, path, data, ...rest_args)
			return this.qry(db_args[0], db_args.length > 1 ? (typeof db_args[1] === 'function' ? db_args[1](data) : db_args[1]) : data)
		} else {
			return this.rr('JSON.SET', hkey, path, data, ...rest_args)
		}
	}

	rjset_sync(hkey, path, data, ...rest_args) {
		//hkey path [db_query] [db_query_params, Array or function] [callback]
		const db_args = rest_args.length && typeof rest_args[0] === 'string' ? rest_args.splice(0, 2) : false
		path = this._rjpath(path)
		this.r('JSON.SET', hkey, path, data, ...rest_args)
		if (db_args) this.pool.query(db_args[0], db_args.length > 1 ? (typeof db_args[1] === 'function' ? db_args[1](data) : db_args[1]) : data)
	}

	_rjpath(path) {
		if (!path || path === '$') return '$'
		if (path.substr(0, 2) !== '$.') return '$.' + path
		return path
	}

	_rpfx(hkey) {
		let _slot_pfx = ''
		if(hkey.substring(0, 1) === '{') {
			_slot_pfx = '{'
			hkey = hkey.substring(1)
		}
		return _slot_pfx + (hkey.indexOf(this.redisHprefix) !== 0 ? this.redisHprefix : '') + hkey
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

	async transaction(args, ...callbacks) {
		let pool
		if(args && args.pool) {
			pool = args.pool
			delete args.pool
		} else {
			pool = this.pool
		}
		return new Promise((resolve, reject) => {
			callbacks = callbacks.filter((cb) => { return typeof cb === 'function' })
			if (!callbacks.length) return reject(2)
			pool.getConnection((err, db) => {
				if (err || !db) {
					if (!err && !db) err = 'No DB1'
					logger.CLOGJ(logger.lineNumber(new Error(), 2) + ' | ' + err, 'force')
					return (args && args.silent_error ? resolve(null) : reject('1 ' + err))
				}
				db.beginTransaction(async (err) => {
					if (err) {
						db.release()
						logger.CLOGJ(logger.lineNumber(new Error(), 2) + ' | ' + err, 'force')
						return (args && args.silent_error ? resolve(null) : reject('2 ' + err))
					}
					let result = null
					for (let i = 0; i < callbacks.length; ++i) {
						try {
							let _cb = callbacks[i]
							result = _cb(db, args, result)
							if (result instanceof Promise) result = await result
						} catch (_exc) {
							db.rollback(() => {
								db.release()
								logger.CLOGJ(logger.lineNumber(new Error(), 2), _exc, 'force')
								return (args && args.silent_error ? resolve(null) : reject('3 ' + _exc))
							})
							return
						}
					}
					db.commit((err) => {
						if (err) {
							db.rollback(() => {
								db.release()
								logger.CLOGJ(logger.lineNumber(new Error(), 2), err, 'force')
								return (args && args.silent_error ? resolve(null) : reject('4 ' + err))
							})
							return
						}
						db.release()
						return resolve(result)
					})
				})
			})
		})
	}

	show_tables(db, tables) {
		return this.qry("SHOW TABLES", [], res => {			
			if(!res || !res.length) return []
			const _tables = res.map(r => Object.values(r)[0])
			if(tables) tables.push(..._tables)
			return _tables
		}, {db: db})		
	}

	show_fields(table) {
		return this.qry("SHOW COLUMNS FROM `" + table + "`", [], res => {
			if(!res || !res.length) return []
			return res.map(r => r.Field)	
		})
	}

	async rinzset(rkey, members_to_check) {
		const res = await this.rr('zmscore', rkey, ...members_to_check)
		if(!res || !res.length) return promisify([])
		let out = []
		for(let i = 0; i < members_to_check.length; ++i) {
			if(res[i] !== null) out.push(members_to_check[i])
		}
		return promisify(out)
	}	

	async db2zset(q, rkey, q_params, opts) {
		if(!q_params) q_params = []
		if(!opts) opts = {}
		if(!opts.content_key) opts.content_key = 'content'
		if(!opts.score_key) opts.score_key = 'score'
		if(!opts.db_limit) opts.db_limit = 1000
		if(!opts.rlimit) opts.rlimit = 100
		
		let i = 0
		while (true) {
			const _q = `${ q } LIMIT ${ i }, ${ opts.db_limit }`
			let res = await this.qry(_q, q_params)
			if(!res || !res.length) break
			let commands = []
			let to_command = ['zadd', rkey]
			for(let _i = 0; _i < res.length; ++_i) {
				to_command.push(res[_i][opts.score_key], res[_i][opts.content_key])
				if(_i && !(_i % opts.rlimit)) {
					commands.push(to_command)
					to_command = ['zadd', rkey]
				}
			}
			if(to_command.length > 2) commands.push(to_command)

			if(commands.length) await this.rpipemulti(commands)
			i += opts.db_limit
		}		
	}

	async zset2set_scan(source_key, target_key) {
		//slow
		let cursor = '0'
		do {
		  const [new_cursor, members] = await this.rr('zscan', source_key, cursor)
		  if (members.length > 0) await this.rr('sadd', target_key, ...members)		  
		  cursor = new_cursor
		} while (cursor !== '0')
	}	

	async zset_convert(source_key, target_key, command, limit, with_scores) {
		//command: to list - lpush, to set - sadd
		if(!command || command !== 'lpush') command = 'sadd'
		if(!limit) limit = 1000
		let start = 0
		while(true) {
		  let members = []
		  const _end = start + limit
		  let params = ['zrange', source_key, start, _end]
		  if(with_scores) params.push('WITHSCORES')
		  const _members = await this.rr(...params)
		  if (!_members || !_members.length) break
		  if(with_scores) {
            for(let i = 0; i < _members.length; i += 2) {
                members.push(_members[i]+','+_members[i+1])
            }    
		  } else {
			members = _members
		  }
		  await this.rr(command, target_key, ...members)		  
		  start = _end + 1
		}
	}

    setHostSCServer(scServer, scInstanceId, internal_channel_name, internal_channel_event) {
        this.scServer = scServer
		this.intercom = new Intercom(scServer, scInstanceId, internal_channel_name, internal_channel_event)
    }	

    publish(channel, data) {        
		return this.intercom.publish(channel, data)
    }

    insertPlatformEvent(event, data, max_delay) {
        const rkey = 'evnt'+(max_delay && [3,4,5].indexOf(max_delay) !== -1 ? max_delay : '')
        this.r('lpush', rkey, JSON.stringify([event, data]))
    }

    insertPlatformEvents(data, max_delay) {
        //data: [event, data]
        const rkey = 'evnt'+(max_delay && [3,4,5].indexOf(max_delay) !== -1 ? max_delay : '')
        data = data.map(_d => JSON.stringify(_d))
        this.r('lpush', rkey, ...data)
    }

    async insertPlatformEventToDB(event, data, is_multi, max_delay) {
        const q = "INSERT INTO evnt_event (evnt, `data`, date_added, is_multi, max_delay, is_json) VALUES (?, ?, NOW(), ?, ?, 1)"
        return this.qry(q, [event, JSON.stringify(data), is_multi ? 1 : 0, max_delay || null])
    }


}

module.exports = new DbConnection()
