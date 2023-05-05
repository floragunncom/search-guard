It was said that permissions should be assigned to backing indices.

1. Create data stream - testCreateDataStream

    In case of creating data stream it seems like permissions needs to be assigned to data streams not to backing indices.

    Is it ok or not?

   * user with perms to data stream can create data stream when name matches pattern used to assign perms
   * user with perms to backing indices cannot create data stream when name matches pattern used to assign perms - gets 403
   * both users get 400 when data stream with given name already exists, no matter whether its name matches pattern used to assign perms or not
   * both users get 403 when given name doesn't match pattern used to assign perms, message says that user has no access to data stream (not to backing index)


2. Add doc to data stream - testAddDocumentToDataStream

    Should add doc to existing data stream or create data stream and add doc to it in case of missing data stream.

    * User with perms to data stream
      * add doc to missing data stream with name that matches pattern used to assign perms - gets 403, but it seems like data stream is created anyway. Sending same request a second time results in 403 with a different error message.
      * in other cases user gets 403, so it seems to work fine

   * User with perms to backing indices
       * add doc to missing data stream with name that matches pattern used to assign perms - gets 403, it should end with 200 (data stream created, doc added)
       * in other cases user gets 201/403 depending on whether he has permission or not, so it seems to work fine
        

3. Bulk add doc to data stream - testBulkAddDocumentToDataStream

    * User with perms to data stream
      * in all cases: data stream exists/doesn't exist, name matches/doesn't match pattern used to assign perms - gets 200 with 403 for each doc, so it seems to work fine

   * User with perms to backing indices
     * data stream doesn't exist, name matches pattern used to assign perms - gets 200 with 403 for each doc, it should create data stream and add docs to it 
     * in other cases user gets 200 with 201/403 for each doc depending on whether he has permission or not, so it seems to work fine


4. Get data stream - testGetDataStream

    * User with perms to data stream
      * get all data streams, get by name pattern (xxx*) - gets an empty list in all cases, so it seems to work fine
      * get by name - gets 403/404, some messages mention data stream name not backing index name - should we determine that user tries to access data stream and return backing index name in all cases?
      * get by name, data stream does not exist but name matches pattern used to assign perms. It returns 404 no such index - shouldn't it return 403 instead?

   * User with perms to backing indices
     * get all data streams - gets empty list, it should return data streams that user can access
     * get by name pattern that matches data streams which user can access - depending on used pattern it returns list containing data streams or an empty list, it should return data streams in both cases
     * get by name pattern that doesn't match data streams which user can access - gets 200 with empty list, so it seems to work fine
     * get by name - gets 200/403, some messages mention data stream name not backing index name - should we determine that user tries to access data stream and return backing index name in all cases?
     * get by name, data stream doesn't exist but user may access it, it returns 403 - shouldn't it return 404 instead?

5. Delete data stream - testDeleteDataStream

    * User with perms to data stream
      * delete by name pattern - gets 200/403, 200 when pattern does not match any existing data stream, 403 when pattern includes existing data stream(s), should it ignore data streams that user cannot access and return 200 in both cases?
      * delete by name - gets 403/404, some messages mention data stream name not backing index name - should we determine that user tries to access data stream and return backing index name in all cases?
      * delete by name, data stream does not exist but name matches pattern used to assign perms. It returns 404 no such index - shouldn't it return 403 instead?

    * User with perms to backing indices
      * delete by name pattern - gets 200/403, 200 when pattern does not match any existing data stream or matches only data streams that user can access, 403 when pattern includes existing data stream(s) that user cannot access, should it ignore data streams that user cannot access and return 200 in both cases?
      * delete by name - gets 200/403, some messages mention data stream name not backing index name - should we determine that user tries to access data stream and return backing index name in all cases?
      * delete by name, data stream doesn't exist but user may access it, it returns 403 - shouldn't it return 404 instead?


6. Search data stream - testSearchDataStream

    * User with perms to data stream
        * search all data streams, search by name pattern (xxx*) - gets an empty list in all cases, so it seems to work fine
        * search by name - gets 403/404, some messages mention data stream name not backing index name, but it's not an endpoint dedicated for data streams, so I guess it's ok 
      
   * User with perms to backing indices
       * search all data streams, search by name pattern (xxx*) - gets 200 with hits only from data streams that user can access or empty list if pattern does not match any data stream/user has no access to data streams, so it seems to work fine
       * get by name, gets 200/403, 200 when data stream exists and user can access it, 403 in other cases, so it seems to work fine


7. Data stream field capabilities - testFieldCapabilitiesDataStream

    * User with perms to data stream
        * get all, get by name pattern (xxx*) - gets 200 with an empty list in all cases, so it seems to work fine
        * get by name - gets 403/404, some messages mention data stream name not backing index name, but it's not an endpoint dedicated for data streams, so I guess it's ok

   * User with perms to backing indices
       * get all, get by name pattern (xxx*) - 200 with hits only from data streams that user can access or empty list if pattern does not match any data stream/user has no access to matching data streams, so it seems to work fine
       * get by name, gets 200/403, 200 when data stream exists and user can access it, 403 in other cases, so it seems to work fine

8. Get data stream stats - testGetDataStreamStats

    * User with perms to data stream
        * get all, get by name pattern (xxx*) - gets 200/403, 200 when pattern does not match any existing data stream, 403 when pattern includes existing data stream(s), should it return 200 in both cases? - `indices:monitor/data_stream/stats` may be added to AuthorizationConfig: DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS, DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT 
        * get by name - gets 403/404, some messages mention data stream name not backing index name - should we determine that user tries to access data stream and return backing index name in all cases?
        * get by name, data stream does not exist but name matches pattern used to assign perms. It returns 404 no such index - shouldn't it return 403 instead?
      
   * User with perms to backing indices
       * get all, get by name pattern - gets 200/403, 200 when pattern does not match any existing data stream or matches only data streams that user can access, 403 when pattern includes existing data stream(s) that user cannot access, should it return 200 in both cases? - `indices:monitor/data_stream/stats` may be added to AuthorizationConfig: DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS, DEFAULT_IGNORE_UNAUTHORIZED_INDICES_ACTIONS_ALLOWING_EMPTY_RESULT
       * get by name, gets 200/403, 200 when data stream exists and user can access it, 403 in other cases, some messages mention data stream name not backing index name - should we determine that user tries to access data stream and return backing index name in all cases?
       * get by name, data stream doesn't exist but user may access it, it returns 403 - shouldn't it return 404 instead?

9. Rollover data stream - testRolloverDataStream

    * User with perms to data stream
        * all actual results match my expected results, gets 403/404, some messages mention data stream name not backing index name, but it's not an endpoint dedicated for data streams, so I guess it's ok

    * User with perms to backing indices
        * rollover by name, data stream exists (name matches pattern used to assign perms) - gets 404 "reason":"no such index [.force_no_index*], it should return 200
        * other cases: actual results match my expected results, gets 403, some messages mention data stream name not backing index name, but it's not an endpoint dedicated for data streams, so I guess it's ok


10. Reindex data stream - testReindexDataStream

    * User with perms to data stream
        * all actual results match my expected results, gets 403/404, some messages mention data stream name not backing index name, but it's not an endpoint dedicated for data streams, so I guess it's ok 

    * User with perms to backing indices
      * source data stream exists and its name matches pattern used to assign perms, dest name matches pattern used to assign perms, gets 403 saying that user has no access to dest (data-stream-second-reindex-049800c6-e9db-4637-9f94-43ea0b7ef2ba: indices:admin/auto_create), it should return 200
      * other cases: actual results match my expected results, gets 403, some messages mention data stream name not backing index name, but it's not an endpoint dedicated for data streams, so I guess it's ok


11. Open closed backing indices - testOpenClosedBackingIndices

    * User with perms to data stream
        * all actual results match my expected results, gets 403/404, some messages mention data stream name not backing index name, but it's not an endpoint dedicated for data streams, so I guess it's ok

    * User with perms to backing indices
        * all actual results match my expected results, some messages mention data stream name not backing index name, but it's not an endpoint dedicated for data streams, so I guess it's ok
        * gets 200 when data stream exists and user can access it, 403 in other cases


12. Migrate index alias to data stream - testMigrateIndexAliasToDataStream

    * User with perms to backing indices and `indices:admin/data_stream/migrate` assigned to index
        * scenario where alias exists, user has access to the write index, but doesn't have permission to create data stream with name generated by migration (it creates data stream with same name as index alias) - in this case it returns 200 anyway, I would expect 403?
        * other cases: all actual results match my expected results


13. Modify data stream - testModifyDataStream

    * User with perms to backing indices and indices:admin/data_stream/modify assigned to index
      * scenario where data stream and index exist, user has access only to index, when he tries to add index to data stream it returns 200 anyway, I would expect 403?
      * other cases: all actual results match my expected results
