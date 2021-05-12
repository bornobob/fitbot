import mysql.connector
import datetime
from exceptions import (
    MemberNotFoundException,
    MemberAlreadyExistsException
)
import pytz
import contextlib


class Database:
    def __init__(self, options):
        self.options = options

    @contextlib.contextmanager
    def conn(self, buffered=None):
        conn = mysql.connector.connect(**self.options)
        cur = conn.cursor(buffered=buffered)
        try:
            yield conn, cur
        finally:
            cur.close()
            conn.close()

    def add_member(self, discord_id):
        if not self.member_exists(discord_id):
            with self.conn() as (conn, cur):
                sql = 'INSERT INTO members (discord_id, date_joined) ' + \
                      'VALUES (%s, %s);'
                utcnow = datetime.datetime.utcnow()
                utcnow_str = utcnow.strftime('%Y-%m-%d %H:%M:%S')
                cur.execute(sql, (discord_id, utcnow_str))
                conn.commit()
        else:
            raise MemberAlreadyExistsException()

    def member_exists(self, discord_id):
        with self.conn(buffered=True) as (_, cur):
            sql = 'SELECT * FROM members WHERE discord_id = \'%s\';'
            cur.execute(sql, (discord_id,))
            res = cur.rowcount > 0
            return res

    def get_paired_account(self, discord_id):
        with self.conn() as (_, cur):
            member = self.get_member_id(discord_id)
            sql = 'SELECT league_account_id FROM members WHERE id = %s;'
            cur.execute(sql, (member,))
            res = cur.fetchone()[0]
            return res

    def member_has_paired_account(self, discord_id):
        paired = self.get_paired_account(discord_id)
        return paired is not None
    
    def account_is_paired(self, account_id):
        with self.conn(buffered=True) as (_, cur):
            sql = 'SELECT * FROM members WHERE league_account_id = %s;'
            cur.execute(sql, (account_id,))
            res = cur.rowcount > 0
            return res

    def pair_account(self, discord_id, account_id):
        with self.conn() as (conn, cur):
            member = self.get_member_id(discord_id)
            sql = 'UPDATE members SET league_account_id = %s ' + \
                  'WHERE id = \'%s\';'
            cur.execute(sql, (account_id, member))
            conn.commit()

    def last_sync_date(self, discord_id):
        with self.conn() as (_, cur):
            member = self.get_member_id(discord_id)
            sql = 'SELECT last_league_sync, date_joined ' + \
                'FROM members WHERE id = %s;'
            cur.execute(sql, (member,))
            res = cur.fetchone()
            last_sync, joined_at = res[0], res[1]
            if last_sync is not None:
                return pytz.utc.localize(last_sync)
            else:
                return pytz.utc.localize(joined_at)

    def save_sync_date(self, discord_id, date):
        with self.conn() as (conn, cur):
            member = self.get_member_id(discord_id)
            date_str = date.strftime('%Y-%m-%d %H:%M:%S')
            sql = 'UPDATE members SET last_league_sync = %s ' + \
                'WHERE id = %s;'
            cur.execute(sql, (date_str, member))
            conn.commit()

    def get_member_id(self, discord_id):
        if self.member_exists(discord_id):
            with self.conn() as (_, cur):
                sql = 'SELECT id FROM members WHERE discord_id = \'%s\';'
                cur.execute(sql, (discord_id,))
                return cur.fetchone()[0]
        else:
            raise MemberNotFoundException()

    def date_has_pushups(self, member_id, date):
        from_date_str = date.date().strftime('%Y-%m-%d %H:%M:%S')
        day_after = date.date() + datetime.timedelta(days=1)
        to_date_str = day_after.strftime('%Y-%m-%d %H:%M:%S')
        with self.conn(buffered=True) as (_, cur):
            sql = 'SELECT * FROM pushups_done WHERE member_id = %s ' + \
                  'AND date >= %s AND date < %s;'
            cur.execute(sql, (member_id, from_date_str, to_date_str))
            return cur.rowcount > 0

    def date_has_pushups_todo(self, member_id, date):
        from_date_str = date.date().strftime('%Y-%m-%d %H:%M:%S')
        day_after = date.date() + datetime.timedelta(days=1)
        to_date_str = day_after.strftime('%Y-%m-%d %H:%M:%S')
        with self.conn(buffered=True) as (_, cur):
            sql = 'SELECT * FROM pushups_todo WHERE member_id = %s ' + \
                  'AND date >= %s AND date < %s;'
            cur.execute(sql, (member_id, from_date_str, to_date_str))
            return cur.rowcount > 0

    def _new_pushup_done_entry(self, member_id, date, nr_pushups):
        date_str = date.date().strftime('%Y-%m-%d %H:%M:%S')
        with self.conn() as (conn, cur):
            sql = 'INSERT INTO pushups_done (member_id, amount, date) ' + \
                  'VALUES (%s, %s, %s);'
            cur.execute(sql, (member_id, nr_pushups, date_str))
            conn.commit()

    def _new_pushup_todo_entry(self, member_id, date, nr_pushups):
        date_str = date.date().strftime('%Y-%m-%d %H:%M:%S')
        with self.conn() as (conn, cur):
            sql = 'INSERT INTO pushups_todo (member_id, amount, date) ' + \
                  'VALUES (%s, %s, %s);'
            cur.execute(sql, (member_id, nr_pushups, date_str))
            conn.commit()

    def _pushups_done(self, member_id, date):
        date_str = date.date().strftime('%Y-%m-%d %H:%M:%S')
        with self.conn() as (_, cur):
            sql = 'SELECT amount FROM pushups_done ' + \
                  'WHERE member_id = %s AND date = %s;'
            cur.execute(sql, (member_id, date_str))
            return cur.fetchone()[0]

    def _pushups_todo(self, member_id, date):
        date_str = date.date().strftime('%Y-%m-%d %H:%M:%S')
        with self.conn() as (_, cur):
            sql = 'SELECT amount FROM pushups_todo ' + \
                  'WHERE member_id = %s AND date = %s;'
            cur.execute(sql, (member_id, date_str))
            return cur.fetchone()[0]

    def pushups_done(self, discord_id, date):
        member_id = self.get_member_id(discord_id)
        if self.date_has_pushups(member_id, date):
            return self._pushups_done(member_id, date)
        else:
            return 0

    def _update_pushups_done(self, member_id, date, amount):
        date_str = date.date().strftime('%Y-%m-%d %H:%M:%S')
        with self.conn() as (conn, cur):
            sql = 'UPDATE pushups_done SET amount = %s ' + \
                  'WHERE member_id = %s AND date = %s;'
            cur.execute(sql, (amount, member_id, date_str))
            conn.commit()

    def _update_pushups_todo(self, member_id, date, amount):
        date_str = date.date().strftime('%Y-%m-%d %H:%M:%S')
        with self.conn() as (conn, cur):
            sql = 'UPDATE pushups_todo SET amount = %s ' + \
                  'WHERE member_id = %s AND date = %s;'
            cur.execute(sql, (amount, member_id, date_str))
            conn.commit()

    def add_pushups_done(self, discord_id, nr_pushups):
        member_id = self.get_member_id(discord_id)
        now = datetime.datetime.now()
        if not self.date_has_pushups(member_id, now):
            self._new_pushup_done_entry(member_id, now, nr_pushups)
        else:
            already_done = self._pushups_done(member_id, now)
            new_amount = already_done + nr_pushups
            self._update_pushups_done(member_id, now, new_amount)
    
    def add_pushups_todo(self, discord_id, nr_pushups, date):
        if isinstance(date, datetime.date):
            date = datetime.datetime.combine(date, 
                                             datetime.datetime.min.time())
        member_id = self.get_member_id(discord_id)
        if not self.date_has_pushups_todo(member_id, date):
            self._new_pushup_todo_entry(member_id, date, nr_pushups)
        else:
            already_todo = self._pushups_todo(member_id, date)
            new_amount = already_todo + nr_pushups
            self._update_pushups_todo(member_id, date, new_amount)

    def done_pushups(self, member_id):
        with self.conn() as (_, cur):
            sql = 'SELECT amount FROM pushups_done ' + \
                  'WHERE member_id = %s;'
            cur.execute(sql, (member_id,))
            return sum(r[0] for r in cur.fetchall())

    def todo_pushups(self, member_id):
        with self.conn() as (_, cur):
            sql = 'SELECT amount FROM pushups_todo ' + \
                'WHERE member_id = %s;'
            cur.execute(sql, (member_id,))
            return sum(r[0] for r in cur.fetchall())

    def get_net_status(self, discord_id):
        member_id = self.get_member_id(discord_id)
        done = self.done_pushups(member_id)
        todo = self.todo_pushups(member_id)
        return todo - done
