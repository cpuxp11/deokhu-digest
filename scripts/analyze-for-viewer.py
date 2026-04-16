#!/usr/bin/env python3
"""덕후방 다이제스트 뷰어용 JSON 생성.
kakaocli query로 직접 메시지를 가져와서 분석 후 data/YYYY-MM-DD.json 출력.

Usage: python3 analyze-for-viewer.py [YYYY-MM-DD] [--repo-dir /path/to/deokhu-digest]
"""
import json, re, os, sys, subprocess
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Config
KAKAOCLI = '/opt/homebrew/bin/kakaocli'
ROOMS = {
    '18477862036085122': '덕후방',
    '18479572103082596': '입문방',
}
BURST_GAP_SEC = 300  # 5분
MIN_BURST_SIZE = 3
STOP_WORDS = set(
    "그냥 진짜 이거 저거 근데 하는 해서 해요 합니다 그리고 이게 저도 그거 정말 완전 "
    "그런 이런 저건 하면 했는 하고 해도 같은 같이 이렇 그렇 저희 한번 하루 그럼 "
    "그래서 지금 오늘 어제 내일 그래 이제 그것 이것 그런데 이라서 계속 아직 아마 "
    "이미 그래도 혹시 감사합니다 있습니다 있어요 없어요 있는데 있는 없는 하는데 "
    "했는데 되는데 너무 제가 저는 있을까요 같은데 다른 따로 다시 어떤 일단 서로 "
    "네네 요거 넵넵 됩니다 있으면 좋겠 수도 것도 그거".split()
)
Q_PATTERN = re.compile(r'[?？]|어떻게|방법|될까요|있을까요|가능한가요|안되|안 되|안돼|막혀|못하|고민|추천|해결')
A_PATTERN = re.compile(r'해결|됩니다|가능해요|추천|방법은|하시면|드릴게|이렇게|해보세요|해봤더니|성공|됐어요|쓰시면')


def query_messages(chat_id, target_date):
    """kakaocli query로 특정 날짜 메시지 가져오기."""
    sql = (
        "SELECT chatId, logId, authorId, type, message, sentAt "
        "FROM NTChatMessage "
        "WHERE chatId = {} AND date(sentAt, 'unixepoch', 'localtime') = '{}' "
        "ORDER BY sentAt"
    ).format(chat_id, target_date)
    r = subprocess.run(
        [KAKAOCLI, 'query', sql],
        capture_output=True, text=True, timeout=60
    )
    if r.returncode != 0:
        print(f'[warn] kakaocli query failed for {chat_id}: {r.stderr[:200]}', file=sys.stderr)
        return []
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        return []


def build_bursts(msgs):
    """연속 메시지를 버스트(5분 이내)로 클러스터링."""
    bursts, cur = [], []
    for m in msgs:
        if not cur or m[5] - cur[-1][5] <= BURST_GAP_SEC:
            cur.append(m)
        else:
            if len(cur) >= MIN_BURST_SIZE:
                bursts.append(cur)
            cur = [m]
    if len(cur) >= MIN_BURST_SIZE:
        bursts.append(cur)
    return bursts


def extract_keywords(text):
    """한글 2자 이상 키워드 추출 (불용어 제거)."""
    words = [w for w in re.findall(r'[가-힣]{2,}', text) if w not in STOP_WORDS]
    return [w for w, _ in Counter(words).most_common(4)]


def analyze_burst(burst, burst_idx, room_name):
    """단일 버스트 분석 → dict."""
    start = datetime.fromtimestamp(burst[0][5]).strftime('%H:%M')
    end = datetime.fromtimestamp(burst[-1][5]).strftime('%H:%M')
    users = len(set(m[2] for m in burst))
    text = ' '.join((m[4] or '') for m in burst)

    has_q = bool(Q_PATTERN.search(text))
    has_sol = bool(A_PATTERN.search(text))

    if has_q and has_sol and len(burst) >= 4 and users >= 2:
        qa_flag = 'QA'
    elif has_q and users >= 2:
        qa_flag = 'Q_'
    else:
        qa_flag = '--'

    keywords = extract_keywords(text)
    summary = ' / '.join((m[4] or '')[:40] for m in burst[:3])[:100]

    rating = 3 if len(burst) >= 40 else (2 if len(burst) >= 15 else 1)

    return {
        'room': room_name,
        'burst_id': 'B%02d' % burst_idx,
        'time': start + '~' + end,
        'msg_count': len(burst),
        'user_count': users,
        'qa_flag': qa_flag,
        'keywords': keywords,
        'summary': summary,
        'rating': rating,
    }


def main():
    # Parse args
    kst = timezone(timedelta(hours=9))
    target = sys.argv[1] if len(sys.argv) > 1 else datetime.now(kst).strftime('%Y-%m-%d')

    repo_dir = None
    if '--repo-dir' in sys.argv:
        idx = sys.argv.index('--repo-dir')
        repo_dir = Path(sys.argv[idx + 1])
    else:
        repo_dir = Path.home() / 'deokhu-digest'

    data_dir = repo_dir / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)

    result = {
        'date': target,
        'rooms': {},
        'hot_topics': [],
        'unanswered': [],
        'card_candidates': [],
        'top_keywords': [],
    }
    all_keywords = Counter()

    for chat_id, room_name in ROOMS.items():
        rows = query_messages(chat_id, target)
        if not rows:
            print(f'[{room_name}] {target}: 메시지 0건', file=sys.stderr)
            result['rooms'][room_name] = {'total_msgs': 0, 'text_msgs': 0, 'active_users': 0, 'media': 0}
            continue

        texts = [m for m in rows if m[3] == 1 and m[4]]
        result['rooms'][room_name] = {
            'total_msgs': len(rows),
            'text_msgs': len(texts),
            'active_users': len(set(m[2] for m in texts)),
            'media': len(rows) - len(texts),
        }
        texts.sort(key=lambda x: x[5])

        bursts = build_bursts(texts)
        for i, burst in enumerate(bursts, 1):
            entry = analyze_burst(burst, i, room_name)
            all_keywords.update(entry['keywords'])

            if entry['qa_flag'] == 'Q_':
                result['unanswered'].append({
                    'room': room_name,
                    'time': entry['time'].split('~')[0],
                    'question_summary': entry['summary'][:80],
                    'keywords': entry['keywords'],
                })

            result['card_candidates'].append(entry)

    # Sort candidates by size, assign IDs
    result['card_candidates'].sort(key=lambda x: x['msg_count'], reverse=True)
    for i, c in enumerate(result['card_candidates'], 1):
        c['id'] = i

    # Top 5 → hot_topics
    for rank, c in enumerate(result['card_candidates'][:5], 1):
        result['hot_topics'].append({
            'rank': rank, 'room': c['room'], 'time': c['time'],
            'msg_count': c['msg_count'], 'user_count': c['user_count'],
            'keywords': c['keywords'], 'summary': c['summary'], 'qa_flag': c['qa_flag'],
        })

    result['top_keywords'] = [w for w, _ in all_keywords.most_common(10)]

    # Write JSON
    out_path = data_dir / f'{target}.json'
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'[analyze] {target} → {out_path} ({out_path.stat().st_size} bytes)')

    # Update index.json
    idx_path = data_dir / 'index.json'
    existing = []
    if idx_path.exists():
        try:
            existing = json.loads(idx_path.read_text())
        except Exception:
            pass
    if target not in existing:
        existing.append(target)
        existing.sort(reverse=True)
    idx_path.write_text(json.dumps(existing, ensure_ascii=False), encoding='utf-8')
    print(f'[analyze] index.json updated ({len(existing)} dates)')


if __name__ == '__main__':
    main()
