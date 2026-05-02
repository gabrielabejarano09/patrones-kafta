const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const nodemailer = require('nodemailer');
 
const app = express();
 
// ── Configuración de Email ───────────────────────────────────────────
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS,
  },
});
 
async function sendEmail(to, subject, html) {
  try {
    await transporter.sendMail({ from: process.env.EMAIL_USER, to, subject, html });
    console.log(`[Notification] Email enviado a ${to}`);
  } catch (err) {
    console.error(`[Notification] Error enviando email a ${to}:`, err.message);
  }
}
 
// ── Templates de Email ───────────────────────────────────────────────
function orderCreatedTemplate(event) {
  const itemsList = event.items.map(i =>
    `<tr><td>${i.productName}</td><td>${i.quantity}</td><td>$${Number(i.subtotal).toLocaleString('es-CO')}</td></tr>`
  ).join('');
  return {
    subject: `✅ Orden Confirmada #${event.orderId}`,
    html: `<h2>¡Tu orden fue recibida!</h2>
           <p>Hola <strong>${event.customerName}</strong>,</p>
           <p>Tu orden <strong>${event.orderId}</strong> ha sido registrada.</p>
           <table border='1'><tr><th>Producto</th><th>Cant.</th><th>Subtotal</th></tr>
           ${itemsList}</table>
           <p><strong>Total: $${Number(event.totalAmount).toLocaleString('es-CO')}</strong></p>`
  };
}
 
function paymentProcessedTemplate(event) {
  return {
    subject: `💳 Pago Procesado - Orden #${event.orderId}`,
    html: `<h2>¡Pago confirmado!</h2>
           <p>Hola <strong>${event.customerName}</strong>,</p>
           <p>Tu pago de <strong>$${Number(event.amount).toLocaleString('es-CO')}</strong>
           para la orden <strong>${event.orderId}</strong> fue procesado.</p>
           <p>Pago ID: ${event.paymentId}</p>`
  };
}
 
function shipmentCreatedTemplate(event) {
  return {
    subject: `📦 Tu pedido está en camino - Tracking: ${event.trackingNumber}`,
    html: `<h2>¡Tu pedido va en camino!</h2>
           <p>Hola <strong>${event.customerName}</strong>,</p>
           <p>Orden: <strong>${event.orderId}</strong></p>
           <p>Tracking: <strong>${event.trackingNumber}</strong></p>
           <p>Carrier: ${event.carrier}</p>
           <p>Entrega estimada: <strong>${event.estimatedDelivery}</strong></p>`
  };
}
 
// ── Conexión a Confluent Cloud ───────────────────────────────────────
const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: { mechanism: 'plain', username: process.env.KAFKA_API_KEY, password: process.env.KAFKA_API_SECRET },
  logLevel: logLevel.WARN
});
 
const consumer = kafka.consumer({ groupId: 'notification-group' });
 
async function start() {
  await consumer.connect();
  await consumer.subscribe({ topics: ['orders', 'payments', 'shipments'], fromBeginning: false });
 
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const event = JSON.parse(message.value.toString());
      console.log(`[Notification] Recibido del topico '${topic}': eventType=${event.eventType} orderId=${event.orderId}`);
 
      let emailData = null;
      if (event.eventType === 'OrderCreated') emailData = orderCreatedTemplate(event);
      else if (event.eventType === 'PaymentProcessed') emailData = paymentProcessedTemplate(event);
      else if (event.eventType === 'ShipmentCreated') emailData = shipmentCreatedTemplate(event);
 
      if (emailData && event.customerEmail) {
        await sendEmail(event.customerEmail, emailData.subject, emailData.html);
      }
    }
  });
}
 
const PORT = process.env.NOTIFICATION_PORT || 3005;
app.listen(PORT, () => console.log(`[Notification] Servicio en puerto ${PORT}`));
start().catch(console.error);
