const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
 
const app = express();
app.use(express.json());
 
const pool = new Pool({
  host: process.env.LOGISTICS_DB_HOST,
  database: process.env.LOGISTICS_DB_NAME,
  user: process.env.LOGISTICS_DB_USER,
  password: process.env.LOGISTICS_DB_PASS,
  port: 5432,
  ssl: process.env.LOGISTICS_DB_SSL === 'true' ? { rejectUnauthorized: false } : false
});
 
const kafka = new Kafka({
  clientId: 'shipping-service',
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: { mechanism: 'plain', username: process.env.KAFKA_API_KEY, password: process.env.KAFKA_API_SECRET },
  logLevel: logLevel.WARN
});
 
const consumer = kafka.consumer({ groupId: 'shipping-group' });
const producer = kafka.producer();
 
async function start() {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'payments', fromBeginning: false });
 
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const event = JSON.parse(message.value.toString());
      console.log(`[Shipping] Recibido del topico '${topic}': eventType=${event.eventType} orderId=${event.orderId}`);
 
      if (event.eventType !== 'PaymentProcessed') return;
 
      // Idempotencia
      const exists = await pool.query(
        'SELECT id FROM shipments WHERE order_id = $1', [event.orderId]
      );
      if (exists.rows.length > 0) {
        console.log(`[Shipping] Envio ya existe para ${event.orderId} — ignorando`);
        return;
      }
 
      const shipmentId = `ship_${uuidv4().split('-')[0]}`;
      const trackingNumber = `TRK${Date.now()}`;
      const eventId = `evt_ship_${uuidv4().split('-')[0]}`;
      const estimatedDelivery = new Date(Date.now() + 5 * 24 * 60 * 60 * 1000);
 
      await pool.query(
        `INSERT INTO shipments (id, order_id, tracking_number, status, carrier, estimated_delivery, event_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [shipmentId, event.orderId, trackingNumber, 'SHIPPED', 'DHL Express', estimatedDelivery, eventId]
      );
 
      const shipmentEvent = {
        eventType: 'ShipmentCreated',
        eventId,
        shipmentId,
        orderId: event.orderId,
        customerId: event.customerId,
        customerName: event.customerName,
        customerEmail: event.customerEmail,
        trackingNumber,
        carrier: 'DHL Express',
        estimatedDelivery: estimatedDelivery.toISOString().split('T')[0],
        amount: event.amount,
        timestamp: new Date().toISOString()
      };
 
      await producer.send({
        topic: 'shipments',
        messages: [{ key: event.orderId, value: JSON.stringify(shipmentEvent) }]
      });
 
      console.log(`[Shipping] Envio creado: ${shipmentId} | Tracking: ${trackingNumber}`);
    }
  });
}
 
app.get('/shipments/:orderId', async (req, res) => {
  const result = await pool.query(
    'SELECT * FROM shipments WHERE order_id = $1', [req.params.orderId]
  );
  if (!result.rows.length) return res.status(404).json({ error: 'Envio no encontrado' });
  res.json(result.rows[0]);
});
 
const PORT = process.env.SHIPPING_PORT || 3004;
app.listen(PORT, () => console.log(`[Shipping] Servicio en puerto ${PORT}`));
start().catch(console.error);
