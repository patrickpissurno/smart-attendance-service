(async () => {

    const moment = require('moment');
    const fastify = require('fastify')();
    const concat = require('concat-stream');

    const rabbitmq = await require('amqplib').connect('amqp://localhost');
    const ch = await rabbitmq.createChannel();

    const mongodb = require('mongodb').MongoClient;
    const ObjectID = require('mongodb').ObjectID;
    const db = (await mongodb.connect('mongodb://localhost:27017')).db('smart_attendance');

    ch.assertQueue('predict', { durable: false });
    ch.assertQueue('attendance', { durable: false });

    fastify.register(require('fastify-multipart'));

    fastify.post('/upload-picture', async (req, reply) => {
        let roomID = null;
        let buffer = null;

        const mp = req.multipart(handler, async err => {
            if(err || !roomID || !buffer)
                return reply.status(500).send({ status: 500 });

            let m = moment()
                .utc()
                .year(0)
                .month(0)
                .date(2)
                .day(moment().day() - 1);
            
            let classes = await db.collection('classes').find({ roomID: roomID }).toArray();
            classes = classes.filter(c => {
                for(let i = 0; i<c.startTime.length; i++)
                    if(m.isBetween(moment(c.startTime[i]).utc(), moment(c.endTime[i]).utc()))
                        return true;
                return false;
            });

            // No classes at the moment, just ignore the image
            if(classes.length <= 0)
                return reply.send({ status: 200 });

            let currentClass = classes[0];
            let groupID = currentClass.groupID + "";

            /**
             * Buffer structure
             * From 0 to 3: groupID.length
             * From 4 to 27: currentClass._id
             * From 28 to groupID.length+28: groupID
             * From groupID.length+4 till the end: png image
             */

            let b = Buffer.alloc(4 + 24 + groupID.length + buffer.length);
            b.writeUInt32LE(groupID.length, 0);
            b.write(currentClass._id.toHexString(), 4, 24, 'ascii');
            b.write(groupID, 4 + 24, groupID.length, 'ascii');
            buffer.copy(b, 4 + 24 + groupID.length, 0, buffer.length);

            ch.sendToQueue('predict', b);
            
            return reply.send({ status: 200 });
        });
        
        mp.on('field', function (key, value) {
            if(key == 'room_id')
                roomID = value;
        });
    
        function handler (field, file, filename, encoding, mimetype) {
            file.pipe(concat(b => buffer = b));
        }
    });

    await fastify.listen(3000, '0.0.0.0');
    console.log(`Listening on ${fastify.server.address().port}`);

    ch.consume('attendance', async payload => {
        let temp = payload.content.toString().split(';');
        if(temp.length < 2)
            return ch.ack(payload);

        const classes = db.collection('classes');

        let groupID = temp[0];
        let classID = temp[1];
        console.log(temp);
        let peopleIDs = temp.length < 3 ? [] : temp[2].split(',').filter(x => x.trim().length > 0);

        console.log('Received attendance of class ' + classID);

        let c = await classes.findOne({_id: ObjectID.createFromHexString(classID)});
        if(!c)
            return ch.ack(payload);

        let attendance = c.attendance.filter(x => moment(x.date).isSame(moment(), 'day'));
        if(attendance.length == 0){
            let m = moment()
                .utc()
                .year(0)
                .month(0)
                .date(2)
                .day(moment().day() - 1)

            let date = null;
            for(let i = 0; i<c.startTime.length; i++)
                if(m.isBetween(moment(c.startTime[i]).utc(), moment(c.endTime[i]).utc())){
                    date = moment(c.startTime[i]).utc();
                    break;
                }

            if(!date)
                return ch.ack(payload);
            
            m = moment();
            date = date.year(m.year())
                .month(m.month())
                .date(m.date());

            await classes.findOneAndUpdate({_id: ObjectID.createFromHexString(classID)}, {
                $push: {
                    attendance: {
                        date: date,
                        attendedIDs: peopleIDs.filter(x => c.students.indexOf(x) != -1)
                    }
                }
            });
        }
        else
        {
            attendance = attendance[0];
            peopleIDs.filter(x => c.students.indexOf(x) != -1).forEach(id => attendance.attendedIDs.indexOf(id) == -1 ? attendance.attendedIDs.push(id) : null);
            
            $set = { };
            $set[`attendance.${c.attendance.indexOf(attendance)}.attendedIDs`] = attendance.attendedIDs;

            await classes.findOneAndUpdate({_id: ObjectID.createFromHexString(classID)}, {$set: $set});
        }

        ch.ack(payload)
    });

})();