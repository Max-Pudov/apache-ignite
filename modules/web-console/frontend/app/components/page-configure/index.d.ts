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

import {Observable} from 'rxjs/Observable'
/// <reference path="./types/uirouter.d.ts" />

declare namespace ig {
    namespace config {
        namespace cluster {
            export type DiscoveryKinds = 'Vm'
                | 'Multicast'
                | 'S3'
                | 'Cloud'
                | 'GoogleStorage'
                | 'Jdbc'
                | 'SharedFs'
                | 'ZooKeeper'
                | 'Kubernetes'

            export interface ShortCluster {
                _id: string,
                name: string,
                discovery: DiscoveryKinds,
                caches: number,
                models: number,
                igfs: number
            }
        }
        namespace model {
            type QueryMetadataTypes = 'Annotations' | 'Configuration'
            type DomainModelKinds = 'query' | 'store' | 'both'
            interface KeyField {
                databaseFieldName: string,
                databaseFieldType: string,
                javaFieldName: string,
                javaFieldType: string
            }
            interface ValueField {
                databaseFieldName: string,
                databaseFieldType: string,
                javaFieldName: string,
                javaFieldType: string
            }
            interface Field {
                name: string,
                className: string
            }
            interface Alias {
                field: string,
                alias: string
            }
            type IndexTypes = 'SORTED' | 'FULLTEXT' | 'GEOSPATIAL'
            export interface IndexField {
                _id: string,
                name?: string,
                direction?: boolean
            }
            interface Index {
                _id: string,
                name: string,
                indexType: IndexTypes,
                fields: Array<IndexField>
            }

            export interface DomainModel {
                _id: string,
                space?: string,
                clusters?: Array<string>,
                caches?: Array<string>,
                queryMetadata?: QueryMetadataTypes,
                kind?: DomainModelKinds,
                tableName?: string,
                keyFieldName?: string,
                valueFieldName?: string,
                databaseSchema?: string,
                databaseTable?: string,
                keyType?: string,
                valueType?: string,
                keyFields?: Array<KeyField>,
                valueFields?: Array<ValueField>,
                queryKeyFields?: Array<string>,
                fields?: Array<Field>,
                aliases?: Array<Alias>,
                indexes?: Array<Index>,
                generatePojo?: boolean
            }

            export interface ShortDomainModel {
                _id: string,
                keyType: string,
                valueType: string,
                hasIndex: boolean
            }
        }
    }
}

export as namespace ig
export = ig