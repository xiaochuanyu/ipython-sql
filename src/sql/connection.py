import sqlalchemy
import prettytable
import IPython.display as dis


class Connection(object):
    current = None
    url_to_connections = {}
    name_to_connections = {}

    def __init__(self, connect_str=None, creator=None):
        try:
            if creator:
                engine = sqlalchemy.create_engine(connect_str, creator=creator)
            else:
                engine = sqlalchemy.create_engine(connect_str)
        except:  # TODO: bare except; but what's an ArgumentError?
            print(self._tell_format())
            raise
        self.dialect = engine.url.get_dialect()
        self.metadata = sqlalchemy.MetaData(bind=engine)
        self.name = Connection._assign_name(engine)
        self.session = engine.connect()
        Connection.name_to_connections[self.name] = self
        Connection.url_to_connections[str(self.metadata.bind.url)] = self
        Connection.current = self

    def close(self):
        self.__class__.close_by_descriptor(self)

    @classmethod
    def get(cls, descriptor, creator=None):
        if isinstance(descriptor, Connection):
            cls.current = descriptor
        elif descriptor:
            conn = cls.name_to_connections.get(descriptor) or \
                   cls.url_to_connections.get(descriptor)
            if conn:
                cls.current = conn
            else:
                # http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#custom-dbapi-connect-arguments
                cls.current = Connection(descriptor, creator)
        if cls.current:
            return cls.current
        else:
            cls.show_connections()
            raise Exception("Cannot run query because there is no current connection.\n{}".format(
                cls._tell_format()
            ))

    @classmethod
    def close_by_descriptor(cls, descriptor):
        if isinstance(descriptor, Connection):
            conn = descriptor
        else:
            conn = cls.name_to_connections.get(descriptor) or \
                   cls.url_to_connections.get(descriptor.lower())
        if not conn:
            cls.show_connections()
            raise Exception("Could not find connection.\n")
        cls.name_to_connections.pop(conn.name)
        cls.url_to_connections.pop(str(conn.metadata.bind.url))
        conn.session.close()

    @classmethod
    def show_connections(cls):
        table = prettytable.PrettyTable()
        table.add_column("name", list(cls.name_to_connections.keys()))
        table.add_column("url", list(cls.url_to_connections.keys()))
        dis.display_html(dis.HTML(table.get_html_string()))

    @classmethod
    def _tell_format(cls):
        return "To connect: %sql -c (postgresql|mysql|<dialect>)://username:password@hostname/dbname\n" \
               "Or %sql -c <existing connection name or URL>"

    @classmethod
    def _assign_name(cls, engine):
        core_name = '{}@{}'.format(engine.url.username, engine.url.database)
        incrementer = 1
        name = core_name
        while name in cls.name_to_connections:
            name = '{}_{}'.format(core_name, incrementer)
            incrementer += 1
        return name
