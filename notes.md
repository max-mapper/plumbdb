# deprecated api

    save {"hello": "world"}
      => {"hello": "world", "_id": 1, "_stamp": 1, "_rev": "1-abc"}

    save {"_id": "foo"}
      => {"_id": "foo", "_stamp": 1, "_rev": "1-abc"}

    given {"_id": "foo", "_stamp": 1, "_rev": "1-abc"}
    save {"_id": "foo", "_stamp": 1, "_rev": "1-abc"}
      => {"_id": "foo", "_stamp": 2, "_rev": "2-xyz"}

    given {"_id": "foo", "_stamp": 1, "_rev": "1-abc"}
    save {"_id": "foo", "pizza": "carl"}
      => {"_id": "foo", "_stamp": 2, "_rev": "2-xyz"}

    given {"hello": "world", "_id": 1, "_stamp": 1, "_rev": "1-abc"}
    save {"goodbye": "world", "_id": 1, "_stamp": 1, "_rev": "1-abc"}
      => {"hello": "world", "_id": 1, "_stamp": 2, "_rev": "2-xyz"}
      => {"hello": "world", "_id": 2, "_stamp": 2, "_rev": "1-abc"}
