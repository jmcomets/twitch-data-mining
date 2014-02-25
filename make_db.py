import os
import sys
import glob
import datetime
import sqlite3 as lite
from lxml import etree

def yield_all_streams():
    date = lambda x: datetime.datetime.strptime(x, '%c')
    bool_ = lambda x: x != 'False'
    str_ = lambda x: str(x.encode('utf-8'))
    all_attrs = ((bool_, ('abuse_reported', 'channel_subscription',
                    'embed_enabled', 'featured',)),
                (int, ('video_height', 'video_width', 'broadcast_part',
                    'channel_count', 'channel_view_count', 'embed_count', 'id',
                    'site_count', 'stream_count',)),
                (str_, ('audio_codec', 'title', 'broadcaster', 'category',
                    'channel', 'format', 'geo', 'language', 'meta_game', 'name',
                    'stream_type', 'subcategory', 'video_codec',)),
                (float, ('video_bitrate',)),
                (date, ('up_time',)))
    for fname in glob.glob('data/*.xml'):
        try:
            root = etree.parse(fname)
        except etree.XMLSyntaxError as e:
            print e
            continue
        for stream_node in root.xpath('//stream'):
            stream = {}
            for node in stream_node:
                if node.text is None:
                    continue
                data = None
                for type_, attrs in all_attrs:
                    if node.tag in attrs:
                        data = type_(node.text)
                if data is None:
                    continue
                stream[node.tag] = data
            yield stream

conn = lite.connect('twitch.db')
c = conn.cursor()
try:
    c.execute('drop table streams')
except lite.Error as e:
    pass
try:
    c.execute('create table streams (abuse_reported boolean, channel_subscription boolean, embed_enabled boolean, featured boolean, video_height int, video_width int, broadcast_part int, channel_count int, channel_view_count int, embed_count int, id int not null primary key, site_count int, stream_count int, audio_codec text, title text, broadcaster text, category text, channel text, format text, geo text, language text, meta_game text, name text, stream_type text, subcategory text, video_codec text, video_bitrate real, up_time datetime)')
except lite.Error as e:
    pass

step = 1000
for i, stream in enumerate(yield_all_streams()):
    if i % step == 0:
        print 'Stream #%s' % i
    query = 'INSERT INTO streams (%s) VALUES (%s)' % (','.join(stream.keys()), ','.join(map(lambda x: '"%s"' % str(x).replace('"', '\''), stream.values())))
    try:
        c.execute(query)
    except lite.Error as e:
        print e
        print 'Query:', query
        continue
conn.commit()
conn.close()
