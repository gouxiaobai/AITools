import os, requests, time
from pathlib import Path
from collections import defaultdict
from requests.exceptions import RequestException

p=Path('.env')
if p.exists():
    for line in p.read_text(encoding='utf-8').splitlines():
        s=line.strip()
        if not s or s.startswith('#') or '=' not in s: continue
        k,v=s.split('=',1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

token=os.getenv('NOTION_TOKEN')
ver=os.getenv('NOTION_VERSION','2022-06-28')
DB_TRADE=os.getenv('DB_STD_TRADES_ID','33c225a4-e273-810f-ae9f-d44f9d44d528')
DB_DIV=os.getenv('DB_STD_DIVIDEND_ID','33c225a4-e273-8112-9444-f798532e60cf')
DB_ANNUAL=os.getenv('DB_ANNUAL_ID','33c225a4-e273-8162-8804-dfde58582535')

h={'Authorization':f'Bearer {token}','Notion-Version':ver,'Content-Type':'application/json'}
base='https://api.notion.com/v1'

def req(m,p,j=None):
    for i in range(5):
        try:
            r=requests.request(m,base+p,headers=h,json=j,timeout=30)
            if r.status_code>=400:
                raise RuntimeError(f'{r.status_code} {p} {r.text[:400]}')
            return r.json() if r.text else {}
        except (RequestException, RuntimeError):
            if i==4:
                raise
            time.sleep(1.2*(i+1))

def qall(dbid):
    out=[]; cur=None
    while True:
        payload={'page_size':100}
        if cur: payload['start_cursor']=cur
        d=req('POST',f'/databases/{dbid}/query',payload)
        out.extend(d.get('results',[]))
        if not d.get('has_more'): break
        cur=d.get('next_cursor')
    return out

def p_title(row,name='记录'):
    p=row.get('properties',{}).get(name,{})
    if p.get('type')!='title': return ''
    return ''.join(x.get('plain_text','') for x in p.get('title',[])).strip()

def p_formula_year(row):
    p=row.get('properties',{}).get('年份',{})
    if p.get('type')!='formula':
        return ''
    f=p.get('formula',{})
    if f.get('type')=='string':
        return (f.get('string') or '').strip()
    if f.get('type')=='number' and f.get('number') is not None:
        n=f.get('number')
        return str(int(n)) if float(n).is_integer() else str(n)
    return ''

def p_date_year(row,name='日期'):
    p=row.get('properties',{}).get(name,{})
    if p.get('type')!='date': return ''
    d=(p.get('date') or {}).get('start') or ''
    return d[:4] if len(d)>=4 else ''

def p_rel_ids(row,name):
    p=row.get('properties',{}).get(name,{})
    if p.get('type')!='relation': return []
    return [x.get('id') for x in p.get('relation',[]) if x.get('id')]

def update_page(page_id, props):
    req('PATCH',f'/pages/{page_id}',{'properties':props})

# load rows
tr=qall(DB_TRADE)
dv=qall(DB_DIV)
an=qall(DB_ANNUAL)

# ensure annual year rows for all source years
src_years=set()
for r in tr:
    y=p_formula_year(r) or p_date_year(r,'日期')
    if y: src_years.add(y)
for r in dv:
    y=p_formula_year(r) or p_date_year(r,'日期')
    if y: src_years.add(y)

annual_by_year={p_title(r,'记录'):r.get('id') for r in an if p_title(r,'记录')}
created=0
for y in sorted(src_years):
    if y in annual_by_year:
        continue
    page=req('POST','/pages',{
      'parent':{'database_id':DB_ANNUAL},
      'properties':{'记录':{'title':[{'type':'text','text':{'content':y}}]}}
    })
    annual_by_year[y]=page.get('id')
    created+=1

# refresh annual map in case titles changed
an2=qall(DB_ANNUAL)
annual_by_year={p_title(r,'记录'):r.get('id') for r in an2 if p_title(r,'记录')}

# reverse-link trades -> annual
trade_linked=0
for r in tr:
    y=p_formula_year(r) or p_date_year(r,'日期')
    if not y or y not in annual_by_year:
        continue
    target=annual_by_year[y]
    cur=p_rel_ids(r,'年度收益汇总（标准）')
    if cur==[target]:
        continue
    update_page(r.get('id'),{'年度收益汇总（标准）':{'relation':[{'id':target}]}})
    trade_linked+=1

# reverse-link dividends -> annual
# find relation prop name in dividend DB that points annual
div_db=req('GET',f'/databases/{DB_DIV}')
div_rel_prop=None
for n,info in div_db.get('properties',{}).items():
    if info.get('type')=='relation' and (info.get('relation',{}).get('database_id')==DB_ANNUAL):
        div_rel_prop=n
        break

div_linked=0
if div_rel_prop:
    for r in dv:
        y=p_formula_year(r) or p_date_year(r,'日期')
        if not y or y not in annual_by_year:
            continue
        target=annual_by_year[y]
        cur=p_rel_ids(r,div_rel_prop)
        if cur==[target]:
            continue
        update_page(r.get('id'),{div_rel_prop:{'relation':[{'id':target}]}})
        div_linked+=1

# verify by counting source-side links per year
tr2=qall(DB_TRADE)
dv2=qall(DB_DIV)

trade_counts=defaultdict(int)
for r in tr2:
    y=p_formula_year(r) or p_date_year(r,'日期')
    rel=p_rel_ids(r,'年度收益汇总（标准）')
    if y and rel:
        trade_counts[y]+=1

div_counts=defaultdict(int)
if div_rel_prop:
    for r in dv2:
        y=p_formula_year(r) or p_date_year(r,'日期')
        rel=p_rel_ids(r,div_rel_prop)
        if y and rel:
            div_counts[y]+=1

# print annual rollup snapshot
an3=qall(DB_ANNUAL)

def rv(row,name):
    p=row.get('properties',{}).get(name,{})
    t=p.get('type')
    if t=='rollup':
        ru=p.get('rollup',{})
        return ru.get(ru.get('type'))
    if t=='formula':
        f=p.get('formula',{})
        return f.get(f.get('type'))
    return None

print('CREATED_ANNUAL_ROWS',created)
print('TRADE_RELINKED',trade_linked)
print('DIV_RELINKED',div_linked,'DIV_REL_PROP',div_rel_prop)
for r in sorted(an3,key=lambda x:p_title(x,'记录')):
    y=p_title(r,'记录')
    if not y: continue
    print(y,'trade_links=',trade_counts.get(y,0),'div_links=',div_counts.get(y,0),'auto_real=',rv(r,'自动_已实现收益'),'auto_div=',rv(r,'自动_分红收益'),'total=',rv(r,'总收益'))
