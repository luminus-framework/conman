# luminus-db

Luminus database connection management library

The library creates a connection pool using the [clj-dbcp](https://github.com/kumarshantanu/clj-dbcp) library.

The library wraps [Yesql](https://github.com/krisajenkins/yesql/tree/devel)
to generate connection aware functions.

## Usage

The database connection is initialized by running the `init!` function and
passing it the Yesql quiries file. A dynamic connection atom will be returned
from init. This atom should be used to manage the database connection:

```clojure
(use 'luminus-db.core)

(defonce conn (init! "queries.sql"))
```

Next, the `connect!` function should be called to initialize the database connection.
The function accepts the connection atom returned by the `init!` function.

```clojure
(connect! conn)
```

The connection can be terminated by running the `disconnect! funciton`

```clojure
(disconnect! conn)
```


## License

Copyright © 2015 Dmitri Sotnikov and Carousel Apps Ltd.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
