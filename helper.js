const moment = require('moment-timezone')
const clone = require('rfdc/default')
const crypto = require('crypto')
const { Packr } = require('msgpackr')
const { parse, stringify } = require('flatted')

const fIdx = (ar, func) => {
    let out = []
    out.push(ar.findIndex(func))
    out.push(out[0] > -1 ? ar[out[0]] : null)
    return out
}

const fIdxAll = (ar, func) => {
    let out = []
    for (let i = 0; i < ar.length; ++i) {
        if (func(ar[i])) out.push([i, ar[i]])
    }
    return out
}

const range = (startAt, endAt) => {
    const do_reverse = startAt > endAt
    if (do_reverse) [startAt, endAt] = [...[endAt, startAt]]
    const size = endAt - startAt + 1
    let ret = [...Array(size).keys()].map((i) => i + startAt)
    if (do_reverse) ret.reverse()
    return ret
}

const randint = (n) => Math.trunc(Math.random() * n)

const randint10p = (n) => {
    return randint2(Math.round(n * 0.9), Math.round(n * 1.1))
}

const randint_p = (n, p) => {
    if (p > 1) p *= 0.01
    return randint2(Math.round(n * (1 - p)), Math.round(n * (1 + p)))
}
const randint2 = (min, max) => {
    if (!min) min = 0
    if (!max) {
        if (!min) return 0
        max = min
        min = 0
    }
    if (min > max) [min, max] = [...[max, min]]
    return Math.floor(Math.random() * (max - min + 1)) + min
}

const shuffle = (ar) => {
    let currentIndex = ar.length,
        randomIndex
    while (currentIndex != 0) {
        randomIndex = randint(currentIndex)
        currentIndex--
        ;[ar[currentIndex], ar[randomIndex]] = [
            ar[randomIndex],
            ar[currentIndex],
        ]
    }
    return ar
}

const array_sum = (ar) => {
    return ar
        .map((v) => parseInt(v))
        .filter((v) => v)
        .reduce((_sum, v) => _sum + v, 0)
}

const array_unique = (ar) => {
    return [...new Set(ar)]
}

const keysValuesToObjects = (keys, ...values) => {
    return values.map((vs) => {
        return keys.reduce((obj, key, index) => {
            obj[key] = vs[index] === undefined ? null : vs[index]
            return obj
        }, {})
    })
}

const objectToKvArray = (obj) => {
    return Object.entries(obj).reduce(
        (acc, [key, value]) => acc.concat(key, value),
        []
    )
}

const keysObjectsToValues = (keys, ...objects) => {
    return objects.map((o) =>
        keys.map((key, index) => (o[key] === undefined ? null : o[key]))
    )
}

const mapObject = (obj, func, exclude) => {
    if (!func && !exclude) return obj
    let entries = Object.entries(obj)
    if (exclude) {
        if (typeof exclude === 'function') {
            entries = entries.filter(([key, value]) => !exclude(key, value))
        } else if (Array.isArray(exclude)) {
            entries = entries.filter((_obj) => !exclude.includes(_obj[0]))
        }
    }
    if (func && typeof func === 'function')
        entries = entries.map(([key, value]) => func(key, value))
    return Object.fromEntries(entries)
}

const objectToString = (obj, process_key) => {
    return Object.entries(obj)
        .map(([key, value]) => {
            return (
                (typeof process_key === 'function' ? process_key(key) : key) +
                ':' +
                value
            )
        })
        .join(',')
}

const stringToObject = (str, process_key, value_is_non_numeric) => {
    if (str.length === 0) return {}
    const entries = str.split(',').map((pair) => {
        const [key, value] = pair.split(':')
        return [
            typeof process_key === 'function' ? process_key(key) : key,
            value_is_non_numeric ? value : Number(value),
        ]
    })
    return Object.fromEntries(entries)
}

const flipKeysValues = (obj) => {
    let out = {}
    for (let k in obj) {
        out[obj[k]] = k
    }
    return out
}

const lineNumber = (error) => {
    return error.stack.split('\n')[1].split('\\').pop().slice(0, -1)
}

const validateEmailSimple = (email) => {
    if (typeof email !== 'string' || !email.length) return false
    email = email.trim().toLowerCase()
    if (!/^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$/.test(email))
        return false
    return email
}

class CopyObj {
    copy(in_obj) {
        let out_obj = {}
        for (let k in in_obj) {
            out_obj[k] = in_obj[k]
        }
        return out_obj
    }

    deepCopy(in_obj) {
        return clone(in_obj)
    }
}

const copyObj = new CopyObj()

const _parse_serialized = (s, dflt, func) => {
    let out
    try {
        out = func(s)
    } catch (_exc) {
        out = dflt
    }
    return out
}

const parseJson = (s, dflt) => {
    return _parse_serialized(s, dflt, JSON.parse)
}

const parseFlatted = (s, dflt) => {
    if (s === null) return null
    return _parse_serialized(s, dflt, parse)
}

const stringifyFlatted = (s) => stringify(s)

const ar2null = (ar) => {
    for (let i = 0; i < ar.length; ++i) {
        if (ar[i] === undefined) ar[i] = null
    }
    return ar
}

const delNull = (obj) => {
    if (typeof obj !== 'object') return obj
    const _keys = Object.keys(obj)
    for (let i = 0; i < _keys.length; ++i) {
        if (obj[_keys[i]] instanceof Date)
            obj[_keys[i]] = obj[_keys[i]].toJSON()
        if (obj[_keys[i]] === null) delete obj[_keys[i]]
    }
    return obj
}

const sNull = (obj_in) => {
    if (typeof obj_in !== 'object') return obj_in
    let obj = clone(obj_in)
    const _keys = Object.keys(obj)
    if (
        !obj.__exclude_from_redis ||
        !Array.isArray(obj.__exclude_from_redis) ||
        !obj.__exclude_from_redis.length
    )
        obj.__exclude_from_redis = false
    for (let i = 0; i < _keys.length; ++i) {
        if (
            (obj.__exclude_from_redis &&
                obj.__exclude_from_redis.indexOf(_keys[i]) !== -1) ||
            typeof obj[_keys[i]] === 'function'
        ) {
            delete obj[_keys[i]]
        } else if (obj[_keys[i]] instanceof Date) {
            obj[_keys[i]] = obj[_keys[i]].toJSON()
        } else if (obj[_keys[i]] === undefined || obj[_keys[i]] === null) {
            obj[_keys[i]] = '\0'
        }
    }
    delete obj.__exclude_from_redis
    return obj
}

const unsNull = (obj) => {
    if (typeof obj !== 'object') return obj
    const _keys = Object.keys(obj)
    for (let i = 0; i < _keys.length; ++i) {
        if (obj[_keys[i]] === '\0') obj[_keys[i]] = null
    }
    return obj
}

const hasOne = (func, ar_or_obj) => {
    for (let i in ar_or_obj) {
        if (func(ar_or_obj[i])) return true
    }
    return false
}

const findOne = (func, ar_or_obj) => {
    for (let i in ar_or_obj) {
        if (func(ar_or_obj[i])) return ar_or_obj[i]
    }
    return null
}

const genSalt = (mult, len) => {
    if (!mult) mult = 2
    if (!len) len = 5
    let s = '1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM'
    let s_len = s.length - 1
    let rands = []
    for (let i = 0; i < len * mult; ++i) {
        rands.push(Math.random())
    }

    rands = rands.map((_x) => {
        return Math.floor(_x * (s_len + 1))
    })

    let sAr = []
    for (let i = 0; i < mult; ++i) {
        let _s = ''
        for (let i2 = 0; i2 < len; ++i2) {
            const _start = rands[i2 + i * len]
            _s += s.substring(_start, _start + 1)
        }
        sAr.push(_s)
    }
    return sAr
}

const camelizeObj = (obj) => {
    if (typeof obj === 'string')
        return obj.replace(/_[a-z]/g, (match) =>
            match.substring(1, 2).toUpperCase()
        )
    if (typeof obj !== 'object') return obj
    if (Array.isArray(obj)) {
        let out_obj = []
        for (let i = 0; i < obj.length; ++i) {
            out_obj.push(camelizeObj(obj[i]))
        }
        return out_obj
    }
    let ret = {}
    for (let k in obj) {
        let k_new = camelizeObj(k)
        ret[k_new] = obj[k]
    }
    return ret
}

const isEmptyObj = (obj, check_empty_values) => {
    /*
	if check_empty_values evals to false: 
		return false if obj contains at least one key, no matter the value
		return true if obj contains no keys		
	otherwise: 
		return false if obj contains at least one value that evals to true
		return true if obj contains no keys OR all values eval to false		
	*/
    for (let k in obj) {
        if (check_empty_values) {
            if (obj[k]) return false
        } else {
            return false
        }
    }
    return true
}

const promisify = (x) => {
    return Promise.resolve(x === undefined ? null : x)
}

const promiseTimeout = (func, t) => {
    return new Promise((resolve) => {
        setTimeout(() => {
            resolve(func())
        }, t)
    })
}

const sleep = async (msec) => {
    await new Promise((r) => {
        setTimeout(() => r(), msec)
    })
}

const getPackr = (structure) => {
    opts = {
        useRecords: true,
        useFloat32: true,
        useTimestamp32: true,
        encodeUndefinedAsNil: true,
    }

    opts.structures = [structure]
    return new Packr(opts)
}

const prepPackrPackData = (packr, data) => {
    if (typeof data !== 'object' || Array.isArray(data)) return data
    const structure = packr.structures[0]
    let out = {}

    for (let f of structure) {
        out[f] = data[f] === undefined ? null : data[f]
    }
    return out
}

const encryptCompat = (s, key, iv) => {
    const cipher = crypto.createCipheriv('aes-256-cbc', key, iv)
    let encrypted = cipher.update(s, 'utf8', 'hex')
    encrypted += cipher.final('hex')
    return encrypted
}

const decryptCompat = (s, key, iv) => {
    const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv)
    let decrypted = decipher.update(s, 'hex', 'utf8')
    decrypted += decipher.final('utf8')
    return decrypted
}

const simpleEncrypt = (s, salt) => {
    const algo = 'aes-256-cbc'
    const key = crypto.scryptSync(salt, 'salt', 32)
    const iv = crypto.randomBytes(16)
    const cipher = crypto.createCipheriv(algo, key, iv)
    let encrypted = cipher.update(s, 'utf8', 'hex')
    encrypted += cipher.final('hex')
    return iv.toString('hex') + encrypted
}

const simpleDecrypt = (s, salt) => {
    const algo = 'aes-256-cbc'
    const key = crypto.scryptSync(salt, 'salt', 32)
    const iv = Buffer.from(s.slice(0, 32), 'hex')
    const encryptedText = s.slice(32)
    const decipher = crypto.createDecipheriv(algo, key, iv)
    let decrypted = decipher.update(encryptedText, 'hex', 'utf8')
    decrypted += decipher.final('utf8')
    return decrypted
}

const arraySumsByIndexes = (ar) => {
    let maxIndex = Math.max(...ar.map((_ar) => _ar[0]))
    const sums = Array(maxIndex + 1).fill(0)

    ar.forEach(([index, value]) => {
        sums[index] += value
    })
    return sums
}

const timestampToDate = (toconvert) => {
    if (!toconvert) toconvert = Date.parse(dateTimeZone(new Date()))
    const unixtimestamp = toconvert.toString().substring(0, 10)
    // Convert timestamp to milliseconds
    const date = new Date(unixtimestamp * 1000)

    const year = date.getFullYear()
    const month = ('0' + (date.getMonth() + 1)).slice(-2)
    const day = ('0' + date.getDate()).slice(-2)
    const hours = ('0' + date.getHours()).slice(-2)
    const minutes = ('0' + date.getMinutes()).slice(-2)
    const seconds = ('0' + date.getSeconds()).slice(-2)

    // return date time in yyyy-mm-dd hh:mm:ss format
    return (
        year +
        '-' +
        month +
        '-' +
        day +
        ' ' +
        hours +
        ':' +
        minutes +
        ':' +
        seconds
    )
}

const toDbDatetime = (s) => {
    if (!s) s = new Date()
    const _ts = Date.parse(s + '')
    if (isNaN(_ts)) return null
    return timestampToDate(_ts)
}

const dateTimeZone = (date, timezone) => {
    return moment
        .tz(date, timezone || process.env.TIMEZONE)
        .format('YYYY-MM-DD HH:mm:ss')
}

const timestampToSeconds = (ts) => {
    if (!ts) ts = Date.now()
    return Math.trunc(ts * 0.001)
}

module.exports = {
    fIdx,
    fIdxAll,
    range,
    randint,
    randint2,
    randint_p,
    randint10p,
    shuffle,
    array_sum,
    array_unique,
    keysValuesToObjects,
    keysObjectsToValues,
    objectToKvArray,
    mapObject,
    objectToString,
    stringToObject,
    flipKeysValues,
    timestampToDate,
    timestampToSeconds,
    toDbDatetime,
    lineNumber,
    validateEmailSimple,
    camelizeObj,
    isEmptyObj,
    copyObj,
    parseJson,
    parseFlatted,
    stringifyFlatted,
    ar2null,
    delNull,
    sNull,
    unsNull,
    hasOne,
    findOne,
    genSalt,
    dateTimeZone,
    promisify,
    promiseTimeout,
    sleep,
    getPackr,
    prepPackrPackData,
    encryptCompat,
    decryptCompat,
    simpleEncrypt,
    simpleDecrypt,
    arraySumsByIndexes,
}
