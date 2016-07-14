'use strict';

const Joi = require('joi');

module.exports = {
    host: Joi.alternatives().try(Joi.string().hostname().required(), Joi.array().items(Joi.string().hostname()).required()),
    port: Joi.number().integer().optional(),
    database: Joi.string().required(),
    user: Joi.string().max(32).required(),
    password: Joi.string().required(),
    connectionLimit: Joi.number().integer().required(),
    multipleStatements: Joi.bool().optional()
};
