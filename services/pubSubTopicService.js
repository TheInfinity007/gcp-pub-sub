const { PubSub } = require('@google-cloud/pubsub');

module.exports = class WhatsappAnalyticsTopicService {
    constructor(options) {
        
        const { topicName} = options;
        console.log('topicName', topicName)

        this.topicName = topicName;
        const credentialsPath = './creds/pub-sub-whatsapp-infobip-analytics-adc-credentials.json';
        const pubsubClient = new PubSub({ keyFilename: credentialsPath });
        this.pubSubClient = pubsubClient;
        

        console.log('this.pubSubClient', this.pubSubClient)
    }

    async getPubSubClient() {
        return this.pubSubClient;
    }

    async publishMessage(pubSubClient, topicName, data) {
        const dataBuffer = Buffer.from(data);
        const messageId = await pubSubClient
            .topic(topicName)
            .publish(dataBuffer);
        return messageId;
    }

    /**
     * Send the 'payload' to the topic
     * @param {object|array} payload Payload for the topic
     */
    async send(payload) {
        try {
            if (typeof payload !== 'object') {
                throw Boom.badImplementation('Payload has to be a topic specified object');
            }

            const messageId = await this.publishMessage(this.pubsubClient, this.topicName, JSON.stringify(payload));
            console.log('message pushed', messageId);
            Logger.info('Whatsapp Infobip Analytics Topic Push Success.', { messageId, topicName: this.topicName });
        } catch (err) {
            Logger.error(`Whatsapp Infobip Analytics Topic Push Failed. Error: [${err.message}]`, err, { ...payload, topicName: this.topicName });
        }
    }

    listenForPullMessages (subscriptionName,  timeout) {
        console.log('this.pubSubClient', this.pubSubClient)
        const subscription = this.pubSubClient.subscription(subscriptionName);

        let messageCount = 0;
        const messageHandler = message => {
            // console.log('message', message)
            
            console.log('Object.keys(message)', Object.keys(message))

            console.log(`\n\nReceived message ${message.id}:`);
            console.log(`\n\nmessage.publishTime ${message.publishTime}:`);
            console.log(`\nData: ${message.data}`);
            const data = JSON.parse(message.data);
            console.log(data);
            console.log(data.isWhatsappNumber)
            console.log(`\nAttributes: ${message.attributes}`);
            console.log(`\n ${JSON.stringify(message.attributes)}`)
    
            messageCount += 1;

            // message.ack();
        };

        subscription.on('message', messageHandler);

        setTimeout(() => {
            subscription.removeListener('message', messageHandler);
            console.log(`${messageCount} message(s) received.`);
        }, timeout * 1000);
    }
};