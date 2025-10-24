const { generateFlights } = require('./flightGeneratorNew');
const { deleteGeneratedFlights } = require('./flightDeleterNew');

module.exports = { generateFlights, deleteGeneratedFlights };