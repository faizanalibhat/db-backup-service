const amqp = require('amqplib');

class RabbitMQ {
    constructor() {
        this.connection = null;
        this.publishChannel = null; // Dedicated channel for publishing
        this.consumerChannels = new Map(); // Map of consumerTag to channel
        this.consumers = []; // Store consumer configurations for restarts
        this.exchangeTypes = {
            direct: 'direct',
            topic: 'topic',
            fanout: 'fanout'
        };
        this.url = process.env.RABBITMQ_URL;
        this.connect();
    }

    static getInstance() {
        if (!RabbitMQ.instance) {
            RabbitMQ.instance = new RabbitMQ();
        }
        return RabbitMQ.instance;
    }

    async reconnect(attempt = 1, maxAttempts = 10) {
        if (attempt > maxAttempts) {
            console.error('Max reconnection attempts reached. Shutting down.');
            process.exit(1); // Graceful shutdown; adjust as needed
        }
        const delay = Math.min(1000 * 2 ** attempt, 30000); // Exponential backoff, max 30s
        console.log(`Reconnecting in ${delay}ms (attempt ${attempt})...`);
        await new Promise(resolve => setTimeout(resolve, delay));
        try {
            await this.connect();
        } catch (err) {
            console.error('Reconnection failed:', err.message);
            await this.reconnect(attempt + 1, maxAttempts);
        }
    }

    async createPublishChannel() {
        try {
            if (this.publishChannel) return;

            this.publishChannel = await this.connection.createConfirmChannel();

            this.publishChannel.on('error', (err) => {
                console.error('Publish channel error:', err.message);
                this.publishChannel = null;
                this.recreatePublishChannel();
            });

            this.publishChannel.on('close', () => {
                console.log('Publish channel closed');
                this.publishChannel = null;
                this.recreatePublishChannel();
            });
        } catch (error) {
            console.error('Failed to create publish channel:', error.message);
            await this.reconnect();
        }
    }

    async recreatePublishChannel() {
        await this.reconnect();
    }

    async createConsumerChannel(consumerTag) {
        try {
            const channel = await this.connection.createChannel();
            await channel.prefetch(10); // Limit to 10 unacknowledged messages per consumer

            channel.on('error', (err) => {
                console.error(`Consumer channel error (${consumerTag}):`, err.message);
                this.consumerChannels.delete(consumerTag);
                this.recreateConsumerChannel(consumerTag);
            });

            channel.on('close', () => {
                console.log(`Consumer channel closed (${consumerTag})`);
                this.consumerChannels.delete(consumerTag);
                this.recreateConsumerChannel(consumerTag);
            });

            this.consumerChannels.set(consumerTag, channel);
            return channel;
        } catch (error) {
            console.error(`Failed to create consumer channel (${consumerTag}):`, error.message);
            await this.reconnect();
        }
    }

    async recreateConsumerChannel(consumerTag) {
        await this.reconnect();
    }

    async connect() {
        try {
            if (this.connection && this.publishChannel) return;

            this.connection = await amqp.connect(this.url);
            console.log('[+] Connected to RabbitMQ...');

            this.connection.on('error', (err) => {
                console.error('RabbitMQ Connection Error:', err.message);
                this.connection = null;
                this.publishChannel = null;
                this.consumerChannels.clear();
                this.reconnect();
            });

            this.connection.on('close', () => {
                console.error('RabbitMQ connection closed');
                this.connection = null;
                this.publishChannel = null;
                this.consumerChannels.clear();
                this.reconnect();
            });

            await this.createPublishChannel();
            // Restart consumers
            await this.restartConsumers();
        } catch (error) {
            console.error('Failed to connect to RabbitMQ:', error.message);
            await this.reconnect();
        }
    }

    async restartConsumers() {
        const consumers = [...this.consumers]; // Copy to avoid mutation issues
        this.consumers = [];
        this.consumerChannels.clear();
        for (const consumer of consumers) {
            if (consumer.method === 'consume') {
                await this.consume(consumer.exchange, consumer.bindingKey, consumer.callback, consumer.queueName);
            } else if (consumer.method === 'receive') {
                await this.receive(consumer.queue, consumer.callback);
            } else if (consumer.method === 'intercept') {
                await this.intercept(consumer.exchange, consumer.callback);
            }
        }
    }

    // Direct message publish
    async send(queue, message) {
        if (!this.publishChannel) {
            await this.reconnect();
        }
        await this._assertQueue(queue, this.exchangeTypes.direct, { durable: true, arguments: { 'x-queue-mode': 'lazy' } }, this.publishChannel);
        this.publishChannel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), { persistent: true });
    }

    // Topic exchange publish
    async publish(exchange, routingKey, message) {
        if (!this.publishChannel) {
            await this.reconnect();
        }
        await this._assertExchange(exchange, this.exchangeTypes.topic, { durable: true }, this.publishChannel);
        this.publishChannel.publish(exchange, routingKey, Buffer.from(JSON.stringify(message)), { persistent: true });
    }

    // Consume messages for a topic exchange
    async consume(exchange, bindingKey, callback, queueName = '') {
        if (!this.connection) {
            await this.reconnect();
        }
        const channel = await this.createConsumerChannel(`consume-${exchange}-${queueName}`);
        await this._assertExchange(exchange, this.exchangeTypes.topic, { durable: true }, channel);
        const queue = await channel.assertQueue(queueName, { durable: true, arguments: { 'x-queue-mode': 'lazy' } });
        await channel.bindQueue(queue.queue, exchange, bindingKey);
        const { consumerTag } = await channel.consume(queue.queue, (msg) => {
            try {
                const content = msg.content.toString();
                const parsed = JSON.parse(content);
                callback(parsed, msg, channel);
            } catch (err) {
                console.error(`Consumer error (consume, ${queue.queue}):`, err.message);
                channel.nack(msg, false, false); // Discard bad message
            }
        }, { noAck: false });
        this.consumers.push({ method: 'consume', exchange, bindingKey, callback, queueName, consumerTag });
    }

    // Direct message consume
    async receive(queue, callback) {
        if (!this.connection) {
            await this.reconnect();
        }
        const channel = await this.createConsumerChannel(`receive-${queue}`);
        await this._assertQueue(queue, this.exchangeTypes.direct, { durable: true, arguments: { 'x-queue-mode': 'lazy' } }, channel);
        const { consumerTag } = await channel.consume(queue, (msg) => {
            try {
                const content = msg.content.toString();
                const parsed = JSON.parse(content);
                callback(parsed, msg, channel);
            } catch (err) {
                console.error(`Consumer error (receive, ${queue}):`, err.message);
                channel.nack(msg, false, false); // Discard bad message
            }
        }, { noAck: false });
        this.consumers.push({ method: 'receive', queue, callback, consumerTag });
    }

    // Fanout broadcast
    async broadcast(exchange, message) {
        if (!this.publishChannel) {
            await this.reconnect();
        }
        await this._assertExchange(exchange, this.exchangeTypes.fanout, { durable: true }, this.publishChannel);
        this.publishChannel.publish(exchange, '', Buffer.from(JSON.stringify(message)), { persistent: true });
    }

    // Fanout intercept
    async intercept(exchange, callback) {
        if (!this.connection) {
            await this.reconnect();
        }
        const channel = await this.createConsumerChannel(`intercept-${exchange}`);
        await this._assertExchange(exchange, this.exchangeTypes.fanout, { durable: true }, channel);
        const queue = await channel.assertQueue('', { durable: true, arguments: { 'x-queue-mode': 'lazy' } });
        await channel.bindQueue(queue.queue, exchange, '');
        const { consumerTag } = await channel.consume(queue.queue, (msg) => {
            try {
                const content = msg.content.toString();
                const parsed = JSON.parse(content);
                callback(parsed, msg, channel);
            } catch (err) {
                console.error(`Consumer error (intercept, ${exchange}):`, err.message);
                channel.nack(msg, false, false); // Discard bad message
            }
        }, { noAck: false });
        this.consumers.push({ method: 'intercept', exchange, callback, consumerTag });
    }

    // Internal helper for asserting queues
    async _assertQueue(queue, type, options, channel) {
        if (!channel) {
            await this.reconnect();
        }
        if (!channel) throw new Error('Channel is not initialized.');
        await channel.assertQueue(queue, options);
    }

    // Internal helper for asserting exchanges
    async _assertExchange(exchange, type, options, channel) {
        if (!channel) {
            await this.reconnect();
        }
        if (!channel) throw new Error('Channel is not initialized.');
        await channel.assertExchange(exchange, type, options);
    }
}

module.exports = { mqbroker: RabbitMQ.getInstance() };