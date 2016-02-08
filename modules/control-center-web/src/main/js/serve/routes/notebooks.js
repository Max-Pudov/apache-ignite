/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Fire me up!

module.exports = {
    implements: 'notebooks-routes',
    inject: ['require(express)', 'mongo']
};

module.exports.factory = function (express, mongo) {
    return new Promise((resolve) => {
        const router = express.Router();

        /**
         * Get notebooks names accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/list', function (req, res) {
            var user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
                if (err)
                    return res.status(500).send(err.message);

                var space_ids = spaces.map(function (value) {
                    return value._id;
                });

                // Get all metadata for spaces.
                mongo.Notebook.find({space: {$in: space_ids}}).select('_id name').sort('name').exec(function (err, notebooks) {
                    if (err)
                        return res.status(500).send(err.message);

                    res.json(notebooks);
                });
            });
        });

        /**
         * Get notebook accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/get', function (req, res) {
            var user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
                if (err)
                    return res.status(500).send(err.message);

                var space_ids = spaces.map(function (value) {
                    return value._id;
                });

                // Get all metadata for spaces.
                mongo.Notebook.findOne({space: {$in: space_ids}, _id: req.body.noteId}).exec(function (err, notebook) {
                    if (err)
                        return res.status(500).send(err.message);

                    res.json(notebook);
                });
            });
        });

        /**
         * Save notebook accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/save', function (req, res) {
            var note = req.body;
            var noteId = note._id;

            if (noteId)
                mongo.Notebook.update({_id: noteId}, note, {upsert: true}, function (err) {
                    if (err)
                        return res.status(500).send(err.message);

                    res.send(noteId);
                });
            else
                mongo.Notebook.findOne({space: note.space, name: note.name}, function (err, note) {
                    if (err)
                        return res.status(500).send(err.message);

                    if (note)
                        return res.status(500).send('Notebook with name: "' + note.name + '" already exist.');

                    (new mongo.Notebook(req.body)).save(function (err, note) {
                        if (err)
                            return res.status(500).send(err.message);

                        res.send(note._id);
                    });
                });
        });

        /**
         * Remove notebook by ._id.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/remove', function (req, res) {
            mongo.Notebook.remove(req.body, function (err) {
                if (err)
                    return res.status(500).send(err.message);

                res.sendStatus(200);
            });
        });

        /**
         * Create new notebook for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/new', function (req, res) {
            var user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.findOne({owner: user_id}, function (err, space) {
                if (err)
                    return res.status(500).send(err.message);

                (new mongo.Notebook({space: space.id, name: req.body.name})).save(function (err, note) {
                    if (err)
                        return res.status(500).send(err.message);

                    return res.send(note._id);
                });
            });
        });

        resolve(router);
    });
};
