var _ = require('lodash');
var http = require('http');
var https = require('https');

module.exports = function (RED) {
    "use strict";
    var Influx = require('influx');
    var { InfluxDB, Point } = require('@influxdata/influxdb-client');

    const VERSION_1X = '1.x';
    const VERSION_18_FLUX = '1.8-flux';
    const VERSION_20 = '2.0';
    const STATUS_CHECK_INTERVAL_MS = 30000;

    const STATUS_CONNECTING = { fill: "yellow", shape: "ring", text: "node-red:common.status.connecting" };
    const STATUS_CONNECTED = { fill: "green", shape: "dot", text: "node-red:common.status.connected" };
    const STATUS_DISCONNECTED = { fill: "red", shape: "ring", text: "node-red:common.status.disconnected" };

    function isConnectionError(err) {
        if (!err) {
            return false;
        }
        var code = err.code || (err.cause && err.cause.code);
        if (['ECONNREFUSED', 'ENOTFOUND', 'ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH'].indexOf(code) !== -1) {
            return true;
        }
        var statusCode = err.statusCode || (err.res && err.res.statusCode);
        if (statusCode === 401 || statusCode === 403 || statusCode >= 500) {
            return true;
        }
        if (err.message && /ECONNREFUSED|ENOTFOUND|ETIMEDOUT|ECONNRESET|network|socket hang up|connect/i.test(err.message)) {
            return true;
        }
        return false;
    }

    /**
     * Credentials pasted into Node-RED often include trailing newlines or
     * other control chars that make Node throw ERR_INVALID_CHAR when setting
     * the Authorization header — an uncaught exception that kills the runtime.
     */
    function sanitizeCredential(value) {
        if (value == null) {
            return '';
        }
        return String(value).replace(/[\r\n\0\x08\x0B\x0C\x1F\x7F]/g, '').trim();
    }

    function isValidHttpHeaderValue(value) {
        // Same rule Node uses for setHeader (latin1 printable + tab)
        return typeof value === 'string' && value.length > 0 && !/[^\t\x20-\x7e\x80-\xff]/.test(value);
    }

    function pingHttpUrl(baseUrl, timeoutMs, rejectUnauthorized) {
        return new Promise(function (resolve, reject) {
            try {
                var pingUrl = new URL(baseUrl);
                pingUrl.pathname = '/ping';
                pingUrl.search = '';
                var lib = pingUrl.protocol === 'https:' ? https : http;
                var req = lib.request({
                    protocol: pingUrl.protocol,
                    hostname: pingUrl.hostname,
                    port: pingUrl.port || (pingUrl.protocol === 'https:' ? 443 : 80),
                    path: pingUrl.pathname,
                    method: 'GET',
                    timeout: timeoutMs,
                    rejectUnauthorized: rejectUnauthorized !== false
                }, function (res) {
                    res.resume();
                    if (res.statusCode === 204 || res.statusCode === 200) {
                        resolve();
                    } else {
                        reject(new Error('HTTP ' + res.statusCode));
                    }
                });
                req.on('error', reject);
                req.on('timeout', function () {
                    req.destroy();
                    reject(new Error('timeout'));
                });
                req.end();
            } catch (err) {
                reject(err);
            }
        });
    }

    /**
     * Config node. Currently we only connect to one host.
     */
    function InfluxConfigNode(n) {
        RED.nodes.createNode(this, n);
        this.hostname = n.hostname;
        this.port = n.port;
        this.database = n.database;
        this.name = n.name;
        this.users = {};
        this.connected = false;
        this.currentStatus = STATUS_DISCONNECTED;
        this.checkTimer = null;
        this.checking = false;

        var node = this;
        var clientOptions = null;
        
        if (!n.influxdbVersion) {
            n.influxdbVersion = VERSION_1X;
        }

        var credentials = this.credentials || {};
        var username = sanitizeCredential(credentials.username);
        var password = sanitizeCredential(credentials.password);
        var tokenCredential = sanitizeCredential(credentials.token);

        if (n.influxdbVersion === VERSION_1X) {
            this.usetls = n.usetls;
            if (typeof this.usetls === 'undefined') {
                this.usetls = false;
            }
            // for backward compatibility with old 'protocol' setting
            if (n.protocol === 'https') {
                this.usetls = true;
            }
            if (this.usetls && n.tls) {
                var tlsNode = RED.nodes.getNode(n.tls);
                if (tlsNode) {
                    this.hostOptions = {};
                    tlsNode.addTLSOptions(this.hostOptions);
                }
            }
            this.client = new Influx.InfluxDB({
                hosts: [{
                    host: this.hostname,
                    port: this.port,
                    protocol: this.usetls ? "https" : "http",
                    options: this.hostOptions
                }],
                database: this.database,
                username: username,
                password: password
            });
        } else if (n.influxdbVersion === VERSION_18_FLUX || n.influxdbVersion === VERSION_20) {
            const timeout =  Math.floor(+(n.timeout?n.timeout:10)*1000) // convert from seconds to milliseconds

            const token = n.influxdbVersion === VERSION_18_FLUX ?
                `${username}:${password}` :
                tokenCredential;

            this.url = n.url;
            this.timeout = timeout;
            this.rejectUnauthorized = n.rejectUnauthorized;

            if (!isValidHttpHeaderValue(token)) {
                this.error(RED._("influxdb.errors.invalidtoken"));
                this.client = null;
            } else {
                clientOptions = {
                    url: n.url,
                    token,
                    timeout,
                    rejectUnauthorized: n.rejectUnauthorized
                }
                this.client = new InfluxDB(clientOptions);
            }
        }
        this.influxdbVersion = n.influxdbVersion;

        this.setStatus = function (status) {
            node.currentStatus = status;
            for (var id in node.users) {
                if (Object.prototype.hasOwnProperty.call(node.users, id)) {
                    node.users[id].status(status);
                }
            }
        };

        this.markConnected = function () {
            node.connected = true;
            node.setStatus(STATUS_CONNECTED);
        };

        this.markDisconnected = function () {
            node.connected = false;
            node.setStatus(STATUS_DISCONNECTED);
        };

        this.reportSuccess = function () {
            if (!node.connected) {
                node.markConnected();
            }
        };

        this.reportError = function (err) {
            if (isConnectionError(err)) {
                node.markDisconnected();
                node.checkConnection();
            }
        };

        this.checkConnection = function () {
            if (!node.client || node.checking) {
                return Promise.resolve();
            }
            node.checking = true;
            var checkPromise;

            if (node.influxdbVersion === VERSION_1X) {
                checkPromise = node.client.ping(5000).then(function (hosts) {
                    var online = hosts.some(function (host) { return host.online; });
                    if (!online) {
                        throw new Error('offline');
                    }
                });
            } else {
                checkPromise = pingHttpUrl(node.url, node.timeout || 10000, node.rejectUnauthorized);
            }

            return checkPromise.then(function () {
                node.markConnected();
            }).catch(function () {
                node.markDisconnected();
            }).finally(function () {
                node.checking = false;
            });
        };

        this.startStatusMonitor = function () {
            if (node.checkTimer) {
                return;
            }
            node.setStatus(STATUS_CONNECTING);
            node.checkConnection();
            node.checkTimer = setInterval(function () {
                node.checkConnection();
            }, STATUS_CHECK_INTERVAL_MS);
        };

        this.stopStatusMonitor = function () {
            if (node.checkTimer) {
                clearInterval(node.checkTimer);
                node.checkTimer = null;
            }
        };

        this.register = function (influxNode) {
            node.users[influxNode.id] = influxNode;
            if (Object.keys(node.users).length === 1) {
                node.startStatusMonitor();
            } else {
                influxNode.status(node.currentStatus);
            }
        };

        this.deregister = function (influxNode, done) {
            delete node.users[influxNode.id];
            if (Object.keys(node.users).length === 0) {
                node.stopStatusMonitor();
                node.connected = false;
            }
            if (done) {
                done();
            }
        };

        this.on('close', function (done) {
            node.stopStatusMonitor();
            node.users = {};
            node.connected = false;
            done();
        });
    }

    RED.nodes.registerType("influxdb", InfluxConfigNode, {
        credentials: {
            username: { type: "text" },
            password: { type: "password" },
            token: { type: "password" }
        }
    });

    function isIntegerString(value) {
        return /^-?\d+i$/.test(value);
    }

    function setFieldIntegers(fields) {
        for (const prop in fields) {
            const value = fields[prop];
            if (isIntegerString(value)) {
                fields[prop] = parseInt(value.substring(0,value.length-1));
            }
        }
    }

    function addFieldToPoint(point, name, value) {
        if (name === 'time') {
            point.timestamp(value);
        } else if (typeof value === 'number') {
            point.floatField(name, value);
        } else if (typeof value === 'string') {
            // string values with numbers ending with 'i' are considered integers            
            if (isIntegerString(value)) {
                value = parseInt(value.substring(0,value.length-1));
                point.intField(name, value);
            } else {
                point.stringField(name, value);
            }
        } else if (typeof value === 'boolean') {
            point.booleanField(name, value);
        }
    }

    function addFieldsToPoint(point, fields) {
        for (const prop in fields) {
            const value = fields[prop];
            addFieldToPoint(point, prop, value);
        }
    }

    // write using influx-client-js
    function writePoints(msg, node, done) {
        var measurement = msg.hasOwnProperty('measurement') ? msg.measurement : node.measurement;
        if (!measurement) {
            return done(RED._("influxdb.errors.nomeasurement"));
        }
        try {
            if (_.isArray(msg.payload) && msg.payload.length > 0) {
                // array of arrays: multiple points with fields and tags
                if (_.isArray(msg.payload[0]) && msg.payload[0].length > 0) {
                    msg.payload.forEach(element => {
                        let point = new Point(measurement);
                        let fields = element[0];
                        addFieldsToPoint(point, fields);
                        let tags = element[1];
                        for (const prop in tags) {
                            point.tag(prop, tags[prop]);
                        }
                        node.client.writePoint(point);
                    });
                } else {
                    // array of non-arrays: one point with both fields and tags
                    let point = new Point(measurement);
                    let fields = msg.payload[0];
                    addFieldsToPoint(point, fields);
                    const tags = msg.payload[1];
                    for (const prop in tags) {
                        point.tag(prop, tags[prop]);
                    }
                    node.client.writePoint(point)
                }
            } else {
                // single object: fields only
                if (_.isPlainObject(msg.payload)) {
                    let point = new Point(measurement);
                    let fields = msg.payload;
                    addFieldsToPoint(point, fields);
                    node.client.writePoint(point);
                } else {
                    // just a value
                    let point = new Point(measurement);
                    let value = msg.payload;
                    addFieldToPoint(point, 'value', value);
                    node.client.writePoint(point);
                }
            }
    
            node.client.flush(true).then(() => {
                    done();
                }).catch(error => {
                    msg.influx_error = {
                        errorMessage: error
                    };
                    done(error);
                });
        } catch (error) {
            msg.influx_error = {
                errorMessage: error
            };
            done(error);
        }
    }

    /**
     * Output node to write to a single influxdb measurement
     */
    function InfluxOutNode(n) {
        RED.nodes.createNode(this, n);
        this.measurement = n.measurement;
        this.influxdb = n.influxdb;
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);
        this.precision = n.precision;
        this.retentionPolicy = n.retentionPolicy;

        // 1.8 and 2.0 only
        this.database = n.database;
        this.precisionV18FluxV20 = n.precisionV18FluxV20;
        this.retentionPolicyV18Flux = n.retentionPolicyV18Flux;
        this.org = n.org;
        this.bucket = n.bucket;

        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            this.status(STATUS_DISCONNECTED);
            return;
        }
        if (!this.influxdbConfig.client) {
            this.error(RED._("influxdb.errors.invalidtoken"));
            this.status(STATUS_DISCONNECTED);
            return;
        }
        let version = this.influxdbConfig.influxdbVersion;

        var node = this;
        this.influxdbConfig.register(this);
        this.on('close', function (done) {
            if (node.influxdbConfig) {
                node.influxdbConfig.deregister(node, done);
            } else {
                done();
            }
        });

        if (version === VERSION_1X) {
            var client = this.influxdbConfig.client;

            node.on("input", function (msg, send, done) {
                var measurement;
                var writeOptions = {};

                var measurement = msg.hasOwnProperty('measurement') ? msg.measurement : node.measurement;
                if (!measurement) {
                    return done(RED._("influxdb.errors.nomeasurement"));
                }
                var precision = msg.hasOwnProperty('precision') ? msg.precision : node.precision;
                var retentionPolicy = msg.hasOwnProperty('retentionPolicy') ? msg.retentionPolicy : node.retentionPolicy;

                if (precision) {
                    writeOptions.precision = precision;
                }

                if (retentionPolicy) {
                    writeOptions.retentionPolicy = retentionPolicy;
                }

                // format payload to match new writePoints API
                var points = [];
                var point;
                if (_.isArray(msg.payload) && msg.payload.length > 0) {
                    // array of arrays
                    if (_.isArray(msg.payload[0]) && msg.payload[0].length > 0) {
                        msg.payload.forEach(function (nodeRedPoint) {
                            let fields = _.clone(nodeRedPoint[0])
                            point = {
                                measurement: measurement,
                                fields,
                                tags: nodeRedPoint[1]
                            }
                            setFieldIntegers(point.fields)
                            if (point.fields.time) {
                                point.timestamp = point.fields.time;
                                delete point.fields.time;
                            }
                            points.push(point);
                        });
                    } else {
                        // array of non-arrays, assume one point with both fields and tags
                        let fields = _.clone(msg.payload[0])
                        point = {
                            measurement: measurement,
                            fields,
                            tags: msg.payload[1]
                        };
                        setFieldIntegers(point.fields)
                        if (point.fields.time) {
                            point.timestamp = point.fields.time;
                            delete point.fields.time;
                        }
                        points.push(point);
                    }
                } else {
                    // fields only
                    if (_.isPlainObject(msg.payload)) {
                        let fields = _.clone(msg.payload)
                        point = {
                            measurement: measurement,
                            fields,
                        };
                        setFieldIntegers(point.fields)
                        if (point.fields.time) {
                            point.timestamp = point.fields.time;
                            delete point.fields.time;
                        }
                    } else {
                        // just a value
                        point = {
                            measurement: measurement,
                            fields: { value: msg.payload }
                        };
                        setFieldIntegers(point.fields)
                    }
                    points.push(point);
                }

                client.writePoints(points, writeOptions).then(() => {
                    node.influxdbConfig.reportSuccess();
                    done();
                }).catch(function (err) {
                    msg.influx_error = {
                        statusCode: err.res ? err.res.statusCode : 503
                    }
                    node.influxdbConfig.reportError(err);
                    done(err);
                });
            });
        } else if (version === VERSION_18_FLUX || version === VERSION_20) {
            let bucket = this.bucket;
            if (version === VERSION_18_FLUX) {
                let retentionPolicy = this.retentionPolicyV18Flux ? this.retentionPolicyV18Flux : 'autogen';
                bucket = `${this.database}/${retentionPolicy}`;
            }
            let org = version === VERSION_18_FLUX ? '' : this.org;

            this.client = this.influxdbConfig.client.getWriteApi(org, bucket, this.precisionV18FluxV20);

            node.on("input", function (msg, send, done) {
                writePoints(msg, node, function (err) {
                    if (err) {
                        node.influxdbConfig.reportError(err);
                    } else {
                        node.influxdbConfig.reportSuccess();
                    }
                    done(err);
                });
            });
        }
    }

    RED.nodes.registerType("influxdb out", InfluxOutNode);

    /**
     * Output node to write to multiple InfluxDb measurements
     */
    function InfluxBatchNode(n) {
        RED.nodes.createNode(this, n);
        this.influxdb = n.influxdb;
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);
        this.precision = n.precision;
        this.retentionPolicy = n.retentionPolicy;

        // 1.8 and 2.0
        this.database = n.database;
        this.precisionV18FluxV20 = n.precisionV18FluxV20;
        this.retentionPolicyV18Flux = n.retentionPolicyV18Flux;
        this.org = n.org;
        this.bucket = n.bucket;


        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            this.status(STATUS_DISCONNECTED);
            return;
        }
        if (!this.influxdbConfig.client) {
            this.error(RED._("influxdb.errors.invalidtoken"));
            this.status(STATUS_DISCONNECTED);
            return;
        }
        let version = this.influxdbConfig.influxdbVersion;

        var node = this;
        this.influxdbConfig.register(this);
        this.on('close', function (done) {
            if (node.influxdbConfig) {
                node.influxdbConfig.deregister(node, done);
            } else {
                done();
            }
        });

        if (version === VERSION_1X) {
            var client = this.influxdbConfig.client;

            node.on("input", function (msg, send, done) {
                var writeOptions = {};
                var precision = msg.hasOwnProperty('precision') ? msg.precision : node.precision;
                var retentionPolicy = msg.hasOwnProperty('retentionPolicy') ? msg.retentionPolicy : node.retentionPolicy;

                if (precision) {
                    writeOptions.precision = precision;
                }

                if (retentionPolicy) {
                    writeOptions.retentionPolicy = retentionPolicy;
                }

                client.writePoints(msg.payload, writeOptions).then(() => {
                    node.influxdbConfig.reportSuccess();
                    done();
                }).catch(function (err) {
                    msg.influx_error = {
                        statusCode: err.res ? err.res.statusCode : 503
                    }
                    node.influxdbConfig.reportError(err);
                    done(err);
                });
            });
        } else if (version === VERSION_18_FLUX || version === VERSION_20) {
            let bucket = node.bucket;
            if (version === VERSION_18_FLUX) {
                let retentionPolicy = this.retentionPolicyV18Flux ? this.retentionPolicyV18Flux : 'autogen';
                bucket = `${this.database}/${retentionPolicy}`;
            }
            let org = version === VERSION_18_FLUX ? '' : this.org;

            var client = this.influxdbConfig.client.getWriteApi(org, bucket, this.precisionV18FluxV20);

            node.on("input", function (msg, send, done) {

                msg.payload.forEach(element => {
                    let point = new Point(element.measurement);
        
                    // time is reserved as a field name still! will be overridden by the timestamp below.
                    addFieldsToPoint(point, element.fields);

                    let tags = element.tags;
                    if (tags) {
                        for (const prop in tags) {
                            point.tag(prop, tags[prop]);
                        }
                    }
                    if (element.timestamp) {
                        point.timestamp(element.timestamp);
                    }
                    client.writePoint(point);
                });

                // ensure we write everything including scheduled retries
                client.flush(true).then(() => {
                        node.influxdbConfig.reportSuccess();
                        done();
                    }).catch(error => {
                        msg.influx_error = {
                            errorMessage: error
                        };
                        node.influxdbConfig.reportError(error);
                        done(error);
                    });
            });
        }
    }

    RED.nodes.registerType("influxdb batch", InfluxBatchNode);

    /**
     * Input node to make queries to influxdb
     */
    function InfluxInNode(n) {
        RED.nodes.createNode(this, n);
        this.influxdb = n.influxdb;
        this.query = n.query;
        this.precision = n.precision;
        this.retentionPolicy = n.retentionPolicy;
        this.rawOutput = n.rawOutput;
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);
        this.org = n.org;

        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            this.status(STATUS_DISCONNECTED);
            return;
        }
        if (!this.influxdbConfig.client) {
            this.error(RED._("influxdb.errors.invalidtoken"));
            this.status(STATUS_DISCONNECTED);
            return;
        }

        let version = this.influxdbConfig.influxdbVersion
        var node = this;
        this.influxdbConfig.register(this);
        this.on('close', function (done) {
            if (node.influxdbConfig) {
                node.influxdbConfig.deregister(node, done);
            } else {
                done();
            }
        });

        if (version === VERSION_1X) {
            var client = this.influxdbConfig.client;

            node.on("input", function (msg, send, done) {
                var query;
                var rawOutput;
                var queryOptions = {};
                var precision;
                var retentionPolicy;

                query = msg.hasOwnProperty('query') ? msg.query : node.query;
                if (!query) {
                    return done(RED._("influxdb.errors.noquery"));
                }

                rawOutput = msg.hasOwnProperty('rawOutput') ? msg.rawOutput : node.rawOutput;
                precision = msg.hasOwnProperty('precision') ? msg.precision : node.precision;
                retentionPolicy = msg.hasOwnProperty('retentionPolicy') ? msg.retentionPolicy : node.retentionPolicy;

                if (precision) {
                    queryOptions.precision = precision;
                }

                if (retentionPolicy) {
                    queryOptions.retentionPolicy = retentionPolicy;
                }

                if (rawOutput) {
                    var queryPromise = client.queryRaw(query, queryOptions);
                } else {
                    var queryPromise = client.query(query, queryOptions);
                }

                queryPromise.then(function (results) {
                    msg.payload = results;
                    node.influxdbConfig.reportSuccess();
                    send(msg);
                    done();
                }).catch(function (err) {
                    msg.influx_error = {
                        statusCode: err.res ? err.res.statusCode : 503
                    }
                    node.influxdbConfig.reportError(err);
                    done(err);
                });
            });

        } else if (version === VERSION_18_FLUX || version === VERSION_20) {
            let org = version === VERSION_20 ? this.org : ''
            this.client = this.influxdbConfig.client.getQueryApi(org);

            node.on("input", function (msg, send, done) {
                var query = msg.hasOwnProperty('query') ? msg.query : node.query;
                if (!query) {
                    return done(RED._("influxdb.errors.noquery"));
                }
                var output = [];
                node.client.queryRows(query, {
                    next(row, tableMeta) {
                        var o = tableMeta.toObject(row)
                        output.push(o);
                    },
                    error(error) {
                        msg.influx_error = {
                            errorMessage: error
                        };
                        node.influxdbConfig.reportError(error);
                        done(error);
                    },
                    complete() {
                        msg.payload = output;
                        node.influxdbConfig.reportSuccess();
                        send(msg);
                        done();
                    },
                });
            });
        }
    }

    RED.nodes.registerType("influxdb in", InfluxInNode);
}
