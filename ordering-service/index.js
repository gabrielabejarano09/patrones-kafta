const express = require('express');
const { Kafka, CompressionTypes, logLevel } = require('kafkajs');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
 
const app = express();
app.use(express.json());
 
// ── Conexión a PostgreSQL (DB Comercial) ─────────────────────────────
const pool = new Pool({
  host: process.env.COMMERCIAL_DB_HOST,
  database: process.env.COMMERCIAL_DB_NAME,
  user: process.env.COMMERCIAL_DB_USER,
  password: process.env.COMMERCIAL_DB_PASS,
  port: 5432,
  ssl: process.env.COMMERCIAL_DB_SSL === 'true' ? { rejectUnauthorized: false } : false
});
 
// ── Conexión a Confluent Cloud (Kafka) ───────────────────────────────
const kafka = new Kafka({
  clientId: 'ordering-service',
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
  logLevel: logLevel.WARN
});
 
const producer = kafka.producer();
 
// ── Endpoint: Crear Orden ────────────────────────────────────────────
app.post('/orders', async (req, res) => {
  const { customerId, items } = req.body;
  if (!customerId || !items || items.length === 0) {
    return res.status(400).json({ error: 'customerId e items son requeridos' });
  }
  try {
    // Obtener datos del cliente
    const customerResult = await pool.query(
      'SELECT * FROM customers WHERE id = $1', [customerId]
    );
    if (customerResult.rows.length === 0) {
      return res.status(404).json({ error: 'Cliente no encontrado' });
    }
    const customer = customerResult.rows[0];
 
    // Obtener precios de los productos
    const productIds = items.map(i => i.productId);
    const productResult = await pool.query(
      'SELECT * FROM products WHERE id = ANY($1)', [productIds]
    );
    const productMap = {};
    productResult.rows.forEach(p => { productMap[p.id] = p; });
 
    // Calcular total
    let totalAmount = 0;
    const enrichedItems = items.map(item => {
      const product = productMap[item.productId];
      if (!product) throw new Error(`Producto ${item.productId} no existe`);
      const subtotal = product.price * item.quantity;
      totalAmount += subtotal;
      return { ...item, productName: product.name, unitPrice: product.price, subtotal };
    });
 
    // Persistir la orden
    const orderId = `ord_${uuidv4().split('-')[0]}`;
    const eventId = `evt_order_${uuidv4().split('-')[0]}`;
 
    await pool.query(
      'INSERT INTO orders (id, customer_id, status, total_amount, event_id) VALUES ($1,$2,$3,$4,$5)',
      [orderId, customerId, 'PENDING', totalAmount, eventId]
    );
 
    for (const item of enrichedItems) {
      await pool.query(
        'INSERT INTO order_items (order_id, product_id, product_name, quantity, unit_price, subtotal) VALUES ($1,$2,$3,$4,$5,$6)',
        [orderId, item.productId, item.productName, item.quantity, item.unitPrice, item.subtotal]
      );
    }
 
    // Publicar evento OrderCreated → tópico 'orders'
    const event = {
      eventType: 'OrderCreated',
      eventId,
      orderId,
      customerId,
      customerName: customer.name,
      customerEmail: customer.email,
      items: enrichedItems,
      totalAmount,
      timestamp: new Date().toISOString()
    };
 
    await producer.send({
      topic: 'orders',
      messages: [{ key: orderId, value: JSON.stringify(event) }]
    });
 
    console.log(`[Ordering] Orden creada y publicada: ${orderId} | Total: ${totalAmount}`);
    res.status(201).json({ orderId, status: 'PENDING', totalAmount, event });
 
  } catch (err) {
    console.error('[Ordering] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});
 
// ── Endpoint: Consultar Orden ────────────────────────────────────────
app.get('/orders/:orderId', async (req, res) => {
  const result = await pool.query(
    `SELECT o.*, json_agg(oi) as items
     FROM orders o
     LEFT JOIN order_items oi ON oi.order_id = o.id
     WHERE o.id = $1 GROUP BY o.id`,
    [req.params.orderId]
  );
  if (result.rows.length === 0) return res.status(404).json({ error: 'Orden no encontrada' });
  res.json(result.rows[0]);
});
 
// ── Arrancar ─────────────────────────────────────────────────────────
async function start() {
  await producer.connect();
  const PORT = process.env.ORDERING_PORT || 3001;
  app.listen(PORT, () => console.log(`[Ordering] Servicio en puerto ${PORT}`));
}
 
start().catch(console.error);
