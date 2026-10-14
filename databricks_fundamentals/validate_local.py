"""Offline fixture and notebook checks, not a Databricks integration test."""
import ast
import json
from decimal import Decimal
from pathlib import Path
from collections import Counter, defaultdict

root=Path(__file__).parent
nb=json.loads((root/'notebooks/01_retail_lakehouse.ipynb').read_text())
code=[c for c in nb['cells'] if c['cell_type']=='code']
assert len(code)==17
for c in code:
    ast.parse(c['source'])
    assert c['outputs']==[] and c['execution_count'] is None
rows=[json.loads(x) for x in (root/'data/orders.json').read_text().splitlines()]
customers=[json.loads(x) for x in (root/'data/customers.json').read_text().splitlines()]
products=[json.loads(x) for x in (root/'data/products.json').read_text().splitlines()]
assert len(rows)==23 and len(customers)==10 and len(products)==5
valid=[r for r in rows if r['customer_id'] is not None and int(r['quantity'])>0]
bad=[r for r in rows if r not in valid]
assert len(bad)==2 and {r['order_id'] for r in bad}=={'O9001','O9002'}
unique={json.dumps(r,sort_keys=True):r for r in valid}
assert len(valid)==21 and len(unique)==20
assert len({r['order_id'] for r in unique.values()})==20
customer_keys={r['customer_id'] for r in customers}
product_keys={r['product_id'] for r in products}
assert all(r['customer_id'] in customer_keys and r['product_id'] in product_keys for r in unique.values())
totals=defaultdict(lambda:[0,Decimal(0)])
for r in unique.values():
    if r['status']=='COMPLETED':
        totals[r['order_date']][0]+=1
        totals[r['order_date']][1]+=Decimal(r['quantity'])*Decimal(r['unit_price'])
assert dict(totals)=={'2026-09-01':[8,Decimal(4100)],'2026-09-02':[8,Decimal(3700)]}
assert sum(x[0] for x in totals.values())==16
assert sum(x[1] for x in totals.values())==Decimal(7800)
assert len(rows)==len(unique)+len(bad)+(len(valid)-len(unique))
assert sum(1 for c in code if 'RUN_' in c['source'] and '_FAILURE = False' in c['source'])==3
print('PASS: 17 syntax-checked code cells; fixtures and expected reconciliation verified. Databricks runtime not executed.')
