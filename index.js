require('dotenv').config();
const express = require('express')

const PubSubTopicService = require('./services/pubSubTopicService')

const app = express();

app.get('/', (req, res) => {
    res.send('Welcome Infinity')
})

app.get('/pullMessages', async(req, res) => {
    try {
        const topicName = process.env.PUB_SUB_TOPIC_NAME;
        const subscriptionName = process.env.PUB_SUB_SUBSCRIBER_NAME;
        console.log('topicName', topicName);
        console.log('subscriptionName', subscriptionName)

        const pubSubTopicService = new PubSubTopicService({ topicName })

        await pubSubTopicService.listenForPullMessages(subscriptionName, timeout = 60);
    } catch (err) {
        console.log('err.message', err.message)
        return res.status(500).json({
            success: false,
            message: "Couldn't recieve messages object",
            data: err
        }) 
    }
})

const PORT = 3001;
app.listen(PORT, () => {
    console.log('Server running on port', PORT);
})