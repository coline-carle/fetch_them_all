#!/usr/bin/env python3
import re
import argparse
import http.client
import sqlite3
import xml.etree.ElementTree as ET


api_entrypoint = "us.api.battle.net"

def create_table(db):
  db.execute("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, status INTEGER, data TEXT)")

def parse_args():
  parser = argparse.ArgumentParser(description='dump all world of warcraft items in a sqlite database')
  parser.add_argument('--sqlite', action='store', dest='sqlite', default='items.db', help='sqlite database filename')
  parser.add_argument('--apikey', action='store', dest='apikey', required=True, help='battle.net API key')
  return parser.parse_args()

def get_sitemap_urls(conn, path):
  conn.request("GET", path)
  print("fetching {}".format(path))
  req = conn.getresponse()
  print(req.status, req.reason)
  content = req.read()
  root = ET.fromstring(content)
  locs = root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
  return [loc.text for loc in locs]



def extract_item_id(url):
  parse = re.search(r"item=(\d+)", url)
  if parse:
    return int(parse.group(1))
  return None

def get_item_sitemap_paths(conn):
  urls = get_sitemap_urls(conn, "/sitemap")
  wowhead_url_prefix = "https://www.wowhead.com"
  return [url[len(wowhead_url_prefix):] for url in urls if "sitemap=item/" in url]


def get_itemid_list(conn, path):
  urls = get_sitemap_urls(conn, path)
  return [extract_item_id(url) for url in urls]

def create_item_lines(db, items):
  c = db.cursor()
  for id in items:
    c.execute("INSERT OR IGNORE INTO items (id) VALUES ({})".format(id))
  db.commit()

def fetch_item(conn, apikey, id):
  path = "/wow/item/{}?apikey={}".format(id, apikey)
  conn.request("GET", path)
  req = conn.getresponse()
  print("fetching item {} returned status {} {}".format(id, req.status, req.reason))
  content = req.read()
  return req.status, content

def fill_database_with_wowhead_ids(db):
  conn = http.client.HTTPSConnection("www.wowhead.com")
  item_sitemap_paths = get_item_sitemap_paths(conn)

  for sitemap_path in item_sitemap_paths:
    items = get_itemid_list(conn, sitemap_path)
    create_item_lines(db, items)

def fetch_not_ok_items(db, apikey):
  conn = http.client.HTTPSConnection(api_entrypoint)
  rows = db.execute('''
    SELECT
      id
    FROM
      items
    WHERE
      status IS NULL OR status != 200 or status != 404
    ORDER BY
      id
    ''').fetchall()

  print("{} items to fetch".format(len(rows)))
  for row in rows:
    id = row[0]
    status, data = fetch_item(conn, apikey, id)

    cur = db.cursor()

    if status == 200:
      cur.execute('''
        UPDATE items
        SET
          status = 200,
          data = ?
        WHERE
          id = ?
        ''', (data, id))
    else:
      cur.execute('''
        UPDATE items
        SET
          status = ?
        wHERE
          id = ?
        ''',
        (status, id))
                                    
    db.commit()                   
  

def main():
  args = parse_args()

  db = sqlite3.connect(args.sqlite)
  create_table(db)

  
  fill_database_with_wowhead_ids(db)
  fetch_not_ok_items(db, args.apikey)


if __name__ == "__main__":
  main()