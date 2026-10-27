#!/usr/bin/env python3
"""Dopecast station catalogue.

Collects candidate stations for the app's genres, checks every stream for real
(it must answer and actually send audio), notes bitrate, codec and whether it
announces track titles, and writes catalog.json, best stations first.

Standard library only. Run daily by .github/workflows/catalog.yml.
"""
import concurrent.futures as cf
import http.client
import json
import os
import re
import socket
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

UA = "Dopecast-catalog/1.0 (+https://github.com)"
PER_GENRE = 400            # stations kept per genre
PER_TAG = 300              # candidates asked from the directory per tag
WORKERS = 128
TIMEOUT = 8                # seconds per stream check
MIN_BYTES = 16 * 1024      # a live stream sends this much quickly
SPEED_SECONDS = 10         # then it must keep up with real time for this long
MIN_SPEED = 0.95           # delivered / needed; a healthy live stream holds 1.00

GENRES = {
    "Hip-Hop": ["old school hip hop", "hip hop", "hiphop", "rap", "g-funk", "boom bap", "west coast hip hop", "90s hip hop", "jazz rap"],
    "Chicano & Latin": ["chicano", "chicano rap", "latin hip hop", "lowrider", "oldies latino", "latin jazz", "salsa", "latin"],
    "Jazz": ["jazz", "smooth jazz", "nu jazz", "acid jazz", "jazz funk", "bebop", "jazz fusion", "vocal jazz", "big band"],
    "Soul & Funk": ["soul", "funk", "neo soul", "classic soul", "northern soul", "motown", "rnb", "r&b"],
    "Mexican": ["ranchera", "mariachi", "regional mexicano", "norteño", "norteno", "bolero", "cumbia", "musica mexicana", "banda", "grupera"],
    # sub-genres of Other in the app: few stations, best quality only (see SUB_* below)
    "70s": ["70s", "1970s"],
    "80s": ["80s", "1980s"],
    "90s": ["90s", "1990s"],
    "Rock 80–90": ["classic rock", "80s rock", "90s rock", "hard rock"],
    "Rhythm & Blues": ["rhythm and blues", "classic r&b", "blues"],
    "Metal": ["heavy metal", "metal", "thrash metal"],
    "Easy Listening": ["easy listening", "soft rock", "beautiful music"],
    "Lounge": ["lounge", "bossa nova", "cocktail"],
    "Other": ["eclectic", "downtempo", "lounge", "chillout", "trip hop", "ambient", "college radio"],
}
SUBS = {"70s", "80s", "90s", "Rock 80–90", "Rhythm & Blues", "Metal", "Easy Listening", "Lounge"}
SUB_LIMIT = 60
# Strict genres: a station is listed only after listen.py has proven it plays the genre
# (every genre except Other). New stations are listened to via listen/pool.json and join once proven.
MIN_KNOWN = 10             # identified plays needed before judging
MIN_FIT = 0.75             # share of plays on genre
MAX_POP = 0.15             # share of modern mainstream (debut 2005+, 1M+ fans); not judged until listen.py knows
MIN_PROVEN = 10            # fewer proven than this (a new genre, first days): old ranking is used and a warning printed
CHART = re.compile(r"\bhits?\b|\bcharts?\b|top ?(40|100)|\btop rap|\btrap|turn ?up|pregame", re.I)   # chart / modern names: never in strict genres
POOL_LIMIT, SUB_POOL_LIMIT = 700, 200   # working candidates listen.py listens to
PLAYLISTS = {  # open, regularly refreshed playlists by genre (github.com/junguler/m3u-radio-music-playlists)
    "Jazz": ["jazz", "smooth_jazz", "acid_jazz", "big_band"],
    "Hip-Hop": ["hip_hop", "rap", "urban"],
    "Chicano & Latin": ["latin", "salsa"],
    "Soul & Funk": ["soul", "funk", "rnb"],
    "Other": ["lounge", "downtempo", "chillout", "college"],
}
RAW = "https://raw.githubusercontent.com/junguler/m3u-radio-music-playlists/main/{}.m3u"
DIRECTORY = ["https://all.api.radio-browser.info", "https://de1.api.radio-browser.info",
             "https://de2.api.radio-browser.info", "https://fi1.api.radio-browser.info", "https://nl1.api.radio-browser.info"]
BLOCK = re.compile(r"соловь|вести|радио россии|россия|sputnik|звезда|маяк|rt news|патриот|свобод|svoboda|liberty|meduza|медуз|эхо москв|echo of moscow|дождь|dozhd|настоящее время|current time|голос америки|voice of america", re.I)
NOT_OURS = re.compile(r"\b(top 40|hits|pop|dance|edm|reggaeton|trap|k-pop|house|techno|trance)\b", re.I)
MEXICAN = re.compile(r"mexic|méxico|ranchera|norte[ñn]|banda|grupera|mariachi|cumbia|regional|tejano|duranguense|jalisco|monterrey|guadalajara|tijuana", re.I)


def key(name):
    return re.sub(r"[\W_]+", "", name.lower())


def get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def directory(path):
    for host in DIRECTORY:
        try:
            return json.loads(get(host + path))
        except Exception:
            continue
    return []


def candidates():
    out = {g: [] for g in GENRES}
    for g, tags in GENRES.items():
        for tag in tags:
            q = urllib.parse.quote(tag)
            for x in directory(f"/json/stations/bytagexact/{q}?hidebroken=true&order=votes&reverse=true&limit={PER_TAG}"):
                name = (x.get("name") or "").strip()
                url = x.get("url_resolved") or x.get("url") or ""
                if not name or not url.startswith("http"):
                    continue
                out[g].append({"n": name[:80], "u": url, "s": (tag if tag[:1].isdigit() else tag.title()).replace("Hip Hop", "Hip-Hop").replace("Rnb", "R&B").replace("R&B", "R&B"),
                               "l": x.get("favicon") or "", "city": x.get("state") or x.get("country") or "",
                               "votes": x.get("votes") or 0, "tags": x.get("tags") or ""})
    for g, files in PLAYLISTS.items():
        for f in files:
            try:
                text = get(RAW.format(f)).decode("utf-8", "replace")
            except Exception:
                continue
            name, logo = None, ""
            for line in text.splitlines():
                line = line.strip()
                if line.startswith("#EXTINF"):
                    m = re.search(r'tvg-logo="([^"]*)"', line)
                    logo = m.group(1) if m else ""
                    name = line.split(",", 1)[1].strip() if "," in line else None
                elif line.startswith("http") and name:
                    out[g].append({"n": name[:80], "u": line, "s": f.replace("_", " ").title().replace("Rnb", "R&B"),
                                   "l": logo if logo.startswith("http") else "", "city": "", "votes": 0, "tags": ""})
                    name = None
    # filters and de-duplication (a station lives in one genre only)
    seen, clean = set(), {g: [] for g in GENRES}
    for g in GENRES:
        for s in out[g]:
            text = s["n"] + " " + s["tags"]
            if BLOCK.search(text) or (g != "Other" and g not in SUBS and NOT_OURS.search(s["n"])):
                continue
            if g == "Mexican" and not MEXICAN.search(text):
                continue
            k = key(s["n"])
            if not k or k in seen:
                continue
            seen.add(k)
            clean[g].append(s)
    return clean


CODECS = [("aacp", "AAC"), ("aac", "AAC"), ("mp4", "AAC"), ("mpeg", "MP3"), ("mp3", "MP3"), ("ogg", "OGG"), ("opus", "OPUS"), ("flac", "FLAC"),
          ("mpegurl", "HLS")]


def probe(url):
    """Open the stream like the app does and read a little audio. Returns facts or None."""
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Icy-MetaData": "1"})
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=TIMEOUT, context=ctx) as r:
            if r.status >= 400:
                return None
            h = {k.lower(): v for k, v in r.headers.items()}
            ctype = h.get("content-type", "").lower()
            if "text/html" in ctype or "scpls" in ctype or "x-pls" in ctype:
                return None
            if "mpegurl" in ctype and ".m3u8" not in url.lower():
                return None   # a plain playlist file, not a stream the app can open
            got = 0
            while got < MIN_BYTES and time.time() - t0 < TIMEOUT:
                chunk = r.read(8192)
                if not chunk:
                    break
                got += len(chunk)
            if got < MIN_BYTES and "mpegurl" not in ctype:
                return None
            codec = next((c for k, c in CODECS if k in ctype), "")
            try:
                kbps = int(re.match(r"\d+", h.get("icy-br", "0").split(",")[0]).group(0))
            except Exception:
                kbps = 0
            ms = int((time.time() - t0) * 1000)           # time to the first audio, before the speed check
            if "mpegurl" not in ctype and not keeps_up(r.read, kbps):
                return None
            return {"c": codec, "b": kbps, "m": bool(h.get("icy-metaint")) or "mpegurl" in ctype, "ms": ms}
    except http.client.HTTPException:
        return probe_raw(url, t0)   # old Shoutcast servers answer "ICY 200 OK", which urllib rejects
    except Exception:
        return None


def keeps_up(read, kbps):
    """After the connect burst, does the stream deliver audio at least as fast as it plays?
    Stations that are slower than real time stutter whatever the player's buffer. Unknown bitrate: not judged."""
    if kbps <= 0:
        return True
    rate = kbps * 1000 / 8
    t0, got = time.time(), 0
    try:
        while time.time() - t0 < 3:                     # skip the burst a server sends on connect
            if not read(16384):
                return False
        t0 = time.time()
        while time.time() - t0 < SPEED_SECONDS:
            chunk = read(16384)
            if not chunk:
                return False
            got += len(chunk)
    except Exception:
        return False
    return got / rate / max(time.time() - t0, 1) >= MIN_SPEED


def probe_raw(url, t0):
    """Plain-socket check for ICY servers: status line must say 200 and audio must follow."""
    u = urllib.parse.urlsplit(url)
    port = u.port or (443 if u.scheme == "https" else 80)
    try:
        sock = socket.create_connection((u.hostname, port), timeout=TIMEOUT)
        if u.scheme == "https":
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=u.hostname)
        path = (u.path or "/") + ("?" + u.query if u.query else "")
        sock.sendall(f"GET {path} HTTP/1.0\r\nHost: {u.hostname}\r\nUser-Agent: {UA}\r\nIcy-MetaData: 1\r\n\r\n".encode())
        data = b""
        while len(data) < MIN_BYTES and time.time() - t0 < TIMEOUT:
            chunk = sock.recv(8192)
            if not chunk:
                break
            data += chunk
        head, _, body = data.partition(b"\r\n\r\n")
        first = head.split(b"\r\n", 1)[0]
        if b" 200" not in first or len(body) < MIN_BYTES // 2:
            sock.close()
            return None
        h = {}
        for line in head.split(b"\r\n")[1:]:
            k, _, v = line.decode("latin-1").partition(":")
            h[k.strip().lower()] = v.strip()
        ctype = h.get("content-type", "").lower()
        codec = next((c for k, c in CODECS if k in ctype), "MP3")
        try:
            kbps = int(re.match(r"\d+", h.get("icy-br", "0")).group(0))
        except Exception:
            kbps = 0
        ms = int((time.time() - t0) * 1000)
        ok = keeps_up(sock.recv, kbps)
        sock.close()
        if not ok:
            return None
        return {"c": codec, "b": kbps, "m": bool(h.get("icy-metaint")), "ms": ms}
    except Exception:
        return None


def load(name, default):
    try:
        return json.load(open(name, encoding="utf-8"))
    except Exception:
        return default


def build(limit=PER_GENRE, out_path="catalog.json"):
    # curation.json: reviewed by hand after listening (remove = not our genre at all, move = belongs to another genre)
    cur = load("curation.json", {})
    removed, moved = set(cur.get("remove", [])), cur.get("move", {})
    # listen/report.json: what each station actually played (listen.py, every 3 hours)
    heard = load(os.path.join("listen", "report.json"), {}).get("stations", {})
    cands = candidates()
    for g in list(cands):
        cands[g] = [s for s in cands[g] if s["u"] not in removed]
    for g in list(cands):
        for s in list(cands[g]):
            to = moved.get(s["u"])
            if to and to != g and to in cands:
                cands[g].remove(s)
                cands[to].append(dict(s, s="Soul" if to == "Soul & Funk" else to))
    for g in cands:                                   # a moved station may already be listed there
        seen = set()
        cands[g] = [s for s in cands[g] if not (s["u"] in seen or seen.add(s["u"]))]
    total = sum(len(v) for v in cands.values())
    print(f"candidates: {total}", file=sys.stderr)
    flat = [(g, s) for g, lst in cands.items() for s in lst]
    results = {}
    with cf.ThreadPoolExecutor(WORKERS) as pool:
        futs = {pool.submit(probe, s["u"]): (g, s) for g, s in flat}
        for f in cf.as_completed(futs):
            g, s = futs[f]
            r = f.result()
            if r:
                results[(g, s["u"])] = r
    cat, pool = {}, {}
    for g, lst in cands.items():
        ok = []
        for s in lst:
            r = results.get((g, s["u"]))
            if not r or r["c"] in ("OGG", "OPUS"):
                continue
            if g in SUBS and not (r["b"] >= 128 or (r["c"] == "AAC" and r["b"] >= 64)):
                continue   # sub-genres keep good-quality streams only   # the app plays MP3/AAC/HLS; Ogg is not supported on macOS
            item = {"n": s["n"], "u": s["u"], "s": s["s"], "c": r["c"], "b": r["b"]}
            if s["l"]:
                item["l"] = s["l"]
            if s["city"]:
                item["city"] = s["city"][:40]
            if r["m"]:
                item["m"] = 1
            # ranking: announces track titles (→ covers), quality, popularity, quick to start
            item["_score"] = (2 if r["m"] else 0) + min(r["b"], 320) / 160 + min(s["votes"], 5000) / 2500 - r["ms"] / 4000
            h = heard.get(s["u"])
            if h and h.get("known", 0) >= 8 and h.get("fit") is not None:
                item["_score"] += 3 * (h["fit"] - 0.5)          # plays its genre → up; mostly something else → to the end
            ok.append(item)
        ok.sort(key=lambda x: -x["_score"])
        for x in ok:
            x.pop("_score")
        pool[g] = [{"n": x["n"], "u": x["u"]} for x in ok[:SUB_POOL_LIMIT if g in SUBS else POOL_LIMIT]]
        if g != "Other":
            sure = [x for x in ok if proven(heard.get(x["u"]), g) and not CHART.search(x["n"])]
            if len(sure) >= MIN_PROVEN:
                ok = sure
            else:
                print(f"WARNING {g}: only {len(sure)} proven by listening, old ranking kept for now", file=sys.stderr)
        cat[g] = ok[:SUB_LIMIT if g in SUBS else limit]
        print(f"{g}: {len(cat[g])} listed, {len(pool[g])} listened to, {len(lst)} candidates", file=sys.stderr)
    doc = {"updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "genres": cat}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, separators=(",", ":"))
    os.makedirs("listen", exist_ok=True)
    with open(os.path.join("listen", "pool.json"), "w", encoding="utf-8") as f:
        json.dump({"genres": pool}, f, ensure_ascii=False, separators=(",", ":"))
    return doc


def proven(h, g):
    """listen/report.json says this station plays its genre and not modern mainstream."""
    return bool(h and h.get("genre") == g and h.get("known", 0) >= MIN_KNOWN and (h.get("fit") or 0) >= MIN_FIT
                and (h.get("pop") is None or h["pop"] <= MAX_POP))


if __name__ == "__main__":
    build()
