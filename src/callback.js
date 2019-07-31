module.exports = (error = null, success = null) => {
    return new Promise((resolve, reject) => {
        if (error) {
            return reject(error)
        }

        return resolve(success)
    })
}
