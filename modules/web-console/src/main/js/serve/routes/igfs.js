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

'use strict';

// Fire me up!

module.exports = {
    implements: 'igfs-routes',
    inject: ['require(lodash)', 'require(express)', 'mongo', 'services/space']
};

module.exports.factory = function(_, express, mongo, spaceService) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();
        
        /**
         * Save IGFS.
         */
        router.post('/save', (req, res) => {
            const params = req.body;
            const clusters = params.clusters;

            mongo.Igfs.findOne({space: params.space, name: params.name}).exec()
                .then((_igfs) => {
                    const igfsId = params._id;

                    if (_igfs && igfsId !== _igfs._id.toString())
                        return res.status(500).send('IGFS with name: "' + params.name + '" already exist.');

                    if (params._id) {
                        return mongo.Igfs.update({_id: igfsId}, params, {upsert: true}).exec()
                            .then(() => mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {igfss: igfsId}}, {multi: true}).exec())
                            .then(() => mongo.Cluster.update({_id: {$nin: clusters}}, {$pull: {igfss: igfsId}}, {multi: true}).exec())
                            .then(() => res.send(igfsId));
                    }

                    return (new mongo.Igfs(params)).save()
                        .then((igfs) =>
                            mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {igfss: igfs._id}}, {multi: true}).exec()
                                .then(() => res.send(igfs._id))
                        );
                })
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Remove IGFS by ._id.
         */
        router.post('/remove', (req, res) => {
            const params = req.body;
            const igfsId = params._id;

            mongo.Cluster.update({igfss: {$in: [igfsId]}}, {$pull: {igfss: igfsId}}, {multi: true}).exec()
                .then(() => mongo.Igfs.remove(params).exec())
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Remove all IGFSs.
         */
        router.post('/remove/all', (req, res) => {
            // Get owned space and all accessed space.
            spaceService.spaceIds(req.currentUserId(), req.header('IgniteDemoMode'))
                .then((spaceIds) =>
                    mongo.Cluster.update({space: {$in: spaceIds}}, {igfss: []}, {multi: true}).exec()
                        .then(() => mongo.Igfs.remove({space: {$in: spaceIds}}).exec())
                )
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        factoryResolve(router);
    });
};

