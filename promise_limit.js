// from https://github.com/featurist/promise-limit

module.exports = function (count) {
  if (!count) {
    return function (fn) {
      return fn()
    }
  }
  var outstanding = 0
  var jobs = []

  function remove () {
    outstanding--

    if (outstanding < count) {
      dequeue()
    }
  }

  function dequeue () {
    var job = jobs.shift()
    semaphore.queue = jobs.length

    if (job) {
      run(job.fn).then(job.resolve).catch(job.reject)
    }
  }

  function queue (fn) {
    return new Promise((resolve, reject) => {
      jobs.push({fn: fn, resolve: resolve, reject: reject})
      semaphore.queue = jobs.length
    })
  }

  function run (fn) {
    outstanding++
    try {
      return Promise.resolve(fn()).then(function (result) {
        remove()
        return result
      }, function (error) {
        remove()
        throw error
      })
    } catch (err) {
      remove()
      return Promise.reject(err)
    }
  }

  var semaphore = function (fn) {
    if (outstanding >= count) {
      return queue(fn)
    } else {
      return run(fn)
    }
  }

  semaphore.queue = 0

  semaphore.map = map

  return semaphore
}

function map (items, mapper) {
  var failed = false

  var limit = this

  return Promise.all(items.map(function () {
    return limit(() => {
      if (!failed) {
        return mapper.apply(undefined, arguments).catch(function (e) {
          failed = true
          throw e
        })
      }
    })
  }))
}