const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
 
const app = express();
app.use(express.json());
 
const pool = new Pool({
  host: process.env.COMMERCIAL_DB_HOST,
  database: process.env.COMMERCIAL_DB_NAME,
  user: process.env.COMMERCIAL_DB_USER,
  password: process.env.COMMERCIAL_DB_PASS,
  port: 5432,
  ssl: process.env.COMMERCIAL_DB_SSL === 'true' ? { rejectUnauthorized: false } : false
});
 
const kafka = new Kafka({
  clientId: 'billing-service',
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: { mechanism: 'plain', username: process.env.KAFKA_API_KEY, password: process.env.KAFKA_API_SECRET },
  logLevel: logLevel.WARN
});
 
const consumer = kafka.consumer({ groupId: 'billing-group' });
const producer = kafka.producer();
 
async function start() {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'orders', fromBeginning: false });
 
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      console.log(`[Billing] Recibido del topico '${topic}': eventType=${event.eventType} orderId=${event.orderId}`);
 
      if (event.eventType !== 'OrderCreated') return;
 
      // Idempotencia: verificar si ya procesamos este evento
      const paymentId = `pay_${uuidv4().split('-')[0]}`;
      const eventId = `evt_payment_${uuidv4().split('-')[0]}`;
 
      const exists = await pool.query(
        'SELECT id FROM payments WHERE order_id = $1', [event.orderId]
      );
      if (exists.rows.length > 0) {
        console.log(`[Billing] Pago ya procesado para orden ${event.orderId} — ignorando`);
        return;
      }
 
      // Simular procesamiento de pago
      await pool.query(
        'INSERT INTO payments (id, order_id, amount, status, event_id) VALUES ($1,$2,$3,$4,$5)',
        [paymentId, event.orderId, event.totalAmount, 'PROCESSED', eventId]
      );
 
      await pool.query(
        "UPDATE orders SET status='PAID', updated_at=NOW() WHERE id=$1",
        [event.orderId]
      );
 
      // Publicar PaymentProcessed → tópico 'payments'
      const paymentEvent = {
        eventType: 'PaymentProcessed',
        eventId,
        paymentId,
        orderId: event.orderId,
        customerId: event.customerId,
        customerName: event.customerName,
        customerEmail: event.customerEmail,
        amount: event.totalAmount,
        items: event.items,
        timestamp: new Date().toISOString()
      };
 
      await producer.send({
        topic: 'payments',
        messages: [{ key: event.orderId, value: JSON.stringify(paymentEvent) }]
      });
 
      console.log(`[Billing] Pago procesado: ${paymentId} para orden ${event.orderId}`);
    }
  });
}
 
const PORT = process.env.BILLING_PORT || 3002;
app.listen(PORT, () => console.log(`[Billing] Servicio en puerto ${PORT}`));
start().catch(console.error);
