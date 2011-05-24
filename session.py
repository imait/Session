# -*- coding: utf-8-unix; mode: python -*-
"""Module to provide session with SQLite3.

This module provides class that manage http session.

Classes:
    Session -- class to provide http session with SQLite3
"""

import os
import datetime
import sys
import random
import hashlib
import sqlite3
import pickle
import base64

class Session:

    """Http session with SQLite3.

    Attributes:
        sid -- session id
        dbpath -- path string for databese file of sqlite3
        validity -- validity period of session
        ipmatch -- if it is True then ip matching is checked
        data -- any type object

    Methodes:
        get_id() -- return session id
        get_created_time() -- return created time of session
        get_accessed_time() -- return last accessed time of session
        get_expire_time() -- return time that session will expire
        get_remote_addr() -- return remote address recorded on session
        get_data() -- return data recorded on session
        set_data(data) -- set data on session:
            data -- any type object
        reset_data() -- reset data recorded on session
        save_data() -- save data on session to SQLite database
        delete() -- delete session
        vacuum() -- maintain SQLite database file

    Useage:
        import Cookie
        import Session

        cookie = Cookie.SimpleCookie(os.environ.get(u'HTTP_COOKIE', u''))
        if cookie.has_key(u'session_cookie'):
            sesid = cookie[u'session_cookie'].value
        else:
            sesid = None
        session = session.Session(dbpath='./databese/filename.sqlite3',
                                  sid=sesid,
                                  validity='30 minutes',
                                  ipmatch=True)
        if sesid == session.get_id():
            # valid session
            ...
        else:
            # invalid session
            ...
    """

    def __init__(self, dbpath, sid=None, validity=u'3 hours', ipmatch=False):

        """Constructor of class Session.
        
        Keyword arguments:

        dbpath -- path string for databese file of sqlite3
        sid -- session id (default None)
        validity -- validity period of session (default '3 hours')
        ipmatch -- if it is True then ip matching is checked (default False)

        Arugument validity depends keywords of SQLite's Date and Time Functions.
        It supposes following units of value:

            years
            months
            days
            hours
            minutes
            seconds
        """

        self.sid = sid
        self.dbpath = dbpath
        self.validity = validity
        self.ipmatch = ipmatch
        self.data = None
        
        connection = self._open_db()
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM sqlite_master \
        WHERE type = \'table\' AND name = ?;',
                       (u'sessions',))
        tablecount = cursor.fetchall()
        if len(tablecount) == 0:
            cursor.execute('CREATE TABLE sessions (id PRIMARY KEY, data, \
            created_time, accessed_time, expire_time, remote_addr);')

        cursor.execute('DELETE FROM sessions \
        WHERE expire_time < datetime(\'now\');')

        if isinstance(self.sid, basestring):
            cursor.execute('SELECT id FROM sessions WHERE id = ?;',
                           (self.sid,))
            idcount = cursor.fetchall()
            if len(idcount) == 0:
                self._create_session_id()
                self._insert_session_record(cursor)
            else:
                if self.ipmatch:
                    current_addr = os.environ.get('REMOTE_ADDR', u'')
                    past_addr = self.get_remote_addr()
                    if current_addr == past_addr:
                        self._update_session_record(cursor)
                    else:
                        self._create_session_id()
                        self._insert_session_record(cursor)
                else:
                    self._update_session_record(cursor)
        else:
            self._create_session_id()
            self._insert_session_record(cursor)

        cursor.close()
        connection.commit()
        connection.close()


    def get_id(self):
        """Return session id."""
        return self.sid

    def get_created_time(self):
        """Return created time of session."""
        connection = self._open_db()
        cursor = connection.cursor()
        cursor.execute('SELECT created_time FROM sessions WHERE id = ?;',
                       (self.sid,))
        created_time = cursor.fetchone()
        cursor.close()
        connection.close()
        return created_time[0]

    def get_accessed_time(self):
        """Return last accessed time of session."""
        connection = self._open_db()
        cursor = connection.cursor()
        cursor.execute('SELECT accessed_time FROM sessions WHERE id = ?;',
                       (self.sid,))
        accessed_time = cursor.fetchone()
        cursor.close()
        connection.close()
        return accessed_time[0]
        
    def get_expire_time(self):
        """Return time that session will expire."""
        connection = self._open_db()
        cursor = connection.cursor()
        cursor.execute('SELECT expire_time FROM sessions WHERE id = ?;',
                       (self.sid,))
        expire_time = cursor.fetchone()
        cursor.close()
        connection.close()
        return expire_time[0]

    def get_remote_addr(self):
        """Return remote address recorded on session."""
        connection = self._open_db()
        cursor = connection.cursor()
        cursor.execute('SELECT remote_addr FROM sessions WHERE id = ?;', \
                       (self.sid,))
        remote_addr = cursor.fetchone()
        cursor.close()
        connection.close()
        return remote_addr[0]

    def get_data(self):
        """Return data recorded on session."""
        if self.data is None:
            connection = self._open_db()
            cursor = connection.cursor()
            cursor.execute('SELECT data FROM sessions WHERE id = ?;',
                           (self.sid,))
            data = cursor.fetchone()
            if data is not None:
                data = data[0]
            if data is not None:
                self.data = pickle.loads(base64.decodestring(data))
        return self.data


    def set_data(self, data):
        """Set data on session."""
        self.data = data

    def reset_data(self):
        """Reset data recorded on session."""
        self.data = None

    def save_data(self):
        """Save data on session to SQLite database."""
        data = self.data
        if data is not None:
            data = base64.encodestring(pickle.dumps(data))
            connection = self._open_db()
            cursor = connection.cursor()
            cursor.execute('UPDATE sessions SET data = ? WHERE id = ?;',
                           (data, self.sid))
            cursor.close()
            connection.commit()
            connection.close()

    def delete(self):
        """Delete session."""
        connection = self._open_db()
        cursor = connection.cursor()
        cursor.execute('DELETE FROM sessions WHERE id = ?;',
                       (self.sid,))
        cursor.close()
        connection.commit()
        connection.close()

    def vacuum(self):
        """Maintain SQLite database file."""
        connection = self._open_db()
        cursor = connection.cursor()
        cursor.execute(u'vacuum;')
        cursor.close()
        connection.close()


    # internal methods

    def _open_db(self):
        return sqlite3.connect(self.dbpath)

    def _create_session_id(self):
        connection = self._open_db()
        cursor = connection.cursor()
        while True:
            now = datetime.datetime.today()
            seed = u'%s%s%s' % (str(os.getpid()),
                                str(now.isoformat()),
                                str(random.randint(0, sys.maxint - 1)))
            message = hashlib.new('sha256')
            message.update(seed)
            sid = message.hexdigest()
            cursor.execute('SELECT id FROM sessions WHERE id = ?;',
                           (sid,))
            idcount = cursor.fetchall()
            if len(idcount) == 0:
                self.sid = sid
                break
        cursor.close()
        connection.close()

    def _insert_session_record(self, cursor):
        cursor.execute('INSERT INTO sessions (id, created_time, \
        accessed_time, expire_time, remote_addr) VALUES(?, datetime(\'now\'), \
        datetime(\'now\'), datetime(\'now\', ?), ?);',
                       (self.sid,
                        self.validity,
                        os.environ.get('REMOTE_ADDR', u'')))

    def _update_session_record(self, cursor):
        cursor.execute('UPDATE sessions SET accessed_time = datetime(\'now\'), \
        expire_time = datetime(\'now\', ?), remote_addr = ? \
        where id = ?;', \
                       (self.validity,
                        os.environ.get('REMOTE_ADDR', u''),
                        self.sid))
