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
  clientId: 'inventory-service',
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: { mechanism: 'plain', username: process.env.KAFKA_API_KEY, password: process.env.KAFKA_API_SECRET },
  logLevel: logLevel.WARN
});
 
const consumer = kafka.consumer({ groupId: 'inventory-group' });
 
async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'orders', fromBeginning: false });
 
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const event = JSON.parse(message.value.toString());
      console.log(`[Inventory] Recibido del topico '${topic}': eventType=${event.eventType} orderId=${event.orderId}`);
 
      if (event.eventType !== 'OrderCreated') return;
 
      // Idempotencia: verificar si ya reservamos para esta orden
      const exists = await pool.query(
        'SELECT id FROM inventory_reservations WHERE order_id = $1 LIMIT 1',
        [event.orderId]
      );
      if (exists.rows.length > 0) {
        console.log(`[Inventory] Reserva ya existe para ${event.orderId} — ignorando`);
        return;
      }
 
      // Reservar stock para cada item
      for (const item of event.items) {
        const eventId = `evt_inv_${uuidv4().split('-')[0]}`;
 
        await pool.query(
          `UPDATE inventory
           SET reserved_quantity = reserved_quantity + $1, updated_at = NOW()
           WHERE product_id = $2 AND (stock_quantity - reserved_quantity) >= $1`,
          [item.quantity, item.productId]
        );
 
        await pool.query(
          'INSERT INTO inventory_reservations (order_id, product_id, quantity, event_id) VALUES ($1,$2,$3,$4)',
          [event.orderId, item.productId, item.quantity, eventId]
        );
      }
 
      console.log(`[Inventory] Stock reservado para orden ${event.orderId}`);
    }
  });
}
 
// Endpoint: consultar stock
app.get('/inventory/:productId', async (req, res) => {
  const result = await pool.query(
    'SELECT *, (stock_quantity - reserved_quantity) AS available FROM inventory WHERE product_id = $1',
    [req.params.productId]
  );
  if (!result.rows.length) return res.status(404).json({ error: 'Producto no encontrado' });
  res.json(result.rows[0]);
});
 
const PORT = process.env.INVENTORY_PORT || 3003;
app.listen(PORT, () => console.log(`[Inventory] Servicio en puerto ${PORT}`));
start().catch(console.error);
