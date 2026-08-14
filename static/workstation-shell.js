(()=>{
const $=s=>document.querySelector(s);
const el=(tag,cls,html='')=>{const n=document.createElement(tag);n.className=cls;n.innerHTML=html;return n};
const fmt=n=>Number(n||0).toLocaleString(undefined,{maximumFractionDigits:0});
const bps=n=>`${Number(n||0)>=0?'+':''}${Number(n||0).toFixed(2)} bps`;
let snapshot=null,replay=null,prepareInfo=null,preparePoll=null;

function selectedSymbol(){return window.OF?.S?.selected||snapshot?.status?.symbols?.[0]||''}
function selectedMetrics(){const s=selectedSymbol();return snapshot?.metrics?.[s]||window.OF?.latest?.(s,'metrics')||null}
function splitSymbols(text){return [...new Set(String(text||'').toUpperCase().split(/[\s,]+/).map(x=>x.trim()).filter(Boolean))]}
function isReplayMode(){return (snapshot?.status?.runtime_mode||snapshot?.status?.mode)==='REPLAY'}

const top=el('div','wsTopActions');
top.innerHTML=`<span class="wsMode">MODE <b id="wsMode">...</b></span><a class="wsAction" href="/radar">RADAR</a><button class="wsAction" id="wsNoviceBtn">NOVICE</button><a class="wsAction" href="/learn" target="_blank">TEACHING</a><button class="wsAction replay" id="wsReplayBtn">SIM REPLAY</button>`;
document.body.appendChild(top);

const bar=el('div','wsReplayBar');
bar.innerHTML=`<button data-a="restart">⏮</button><button data-a="prev_bar">◀ BAR</button><button data-a="play" id="wsPlay">▶ PLAY</button><button data-a="next_bar">BAR ▶</button><div class="wsTimeline"><span class="wsTimelineTop" id="wsClock">--:--:-- ET</span><input id="wsSeek" type="range" min="0" max="1000" value="0"><span class="wsTimelineBottom" id="wsPrice">—</span></div><select id="wsSpeed"><option>.25</option><option>.5</option><option selected>1</option><option>2</option><option>5</option><option>10</option><option>25</option><option>100</option></select>`;
document.body.appendChild(bar);

const novice=el('aside','wsNovice');
novice.innerHTML=`<h3>NOVICE // 当前 View 怎么看</h3><h4>CANDLE + FOOTPRINT 回答什么？</h4><ul><li><strong>哪几个价位发生最大交换？</strong> 看每根 Bar 的 POC 与 Volume。</li><li><strong>哪边主动成交占优？</strong> 看 Sell@Bid / Buy@Ask / Δ。</li><li><strong>单边成交是否真的推动价格？</strong> 把 Δ 和左边 Candle 的实际价格结果一起看。</li></ul><h4>最重要的判断顺序</h4><p>① 先看 Candle 有没有价格进展。<br>② 再看同一根 Footprint 谁在主动成交。<br>③ 如果成交方向和价格结果背离，切到 Pressure 检查 Absorption。<br>④ CVD 用于确认/背离，不单独作为方向信号。</p><h4>结构标记</h4><p><strong>青框：</strong>单根 Bar POC。<br><strong>橙框：</strong>Diagonal Imbalance。<br><strong>BID ABS：</strong>负 Δ 但 Candle 上涨。<br><strong>OFFER ABS：</strong>正 Δ 但 Candle 下跌。</p><h4>Replay</h4><p>SIM REPLAY 会在背景逐只下载 Alpaca Historical SIP。左侧 ACTIVE 表示该股票整日录像已准备好；INACTIVE 表示仍在下载或排队。准备期间不会锁住主界面。</p>`;
document.body.appendChild(novice);

const modal=el('div','wsReplayModal');
modal.innerHTML=`<div class="wsReplayDialog"><button class="wsReplayClose" id="wsReplayClose">×</button><div class="wsReplayKicker">GLOBAL MARKET REPLAY</div><h2>准备最近完整交易日</h2><p class="wsReplayLead">输入最多 5 只美股。按下后窗口会立即关闭，数据在背景逐只下载；左侧 Symbol 列表会显示 ACTIVE / INACTIVE 状态。</p><label>Symbols</label><input id="wsReplaySymbols" class="wsReplayInput" autocomplete="off" spellcheck="false" placeholder="XE SNDK NVDA"><div class="wsReplayHint" id="wsReplayHint">最多 5 只；逐只下载，完成即 ACTIVE。</div><div id="wsReplayCredentials"><label>Alpaca API Key</label><input id="wsReplayKey" class="wsReplayInput" type="password" autocomplete="off"><label>Alpaca API Secret</label><input id="wsReplaySecret" class="wsReplayInput" type="password" autocomplete="off"><div class="wsReplayHint">仅发送到本机 Workstation，本页不保存凭证。</div></div><div class="wsReplayError" id="wsReplayError"></div><button class="wsReplayStart" id="wsReplayStart">PREPARE REPLAY</button></div>`;
document.body.appendChild(modal);

const prepPane=el('div','wsReplayPrepPane');
prepPane.innerHTML=`<div class="wsPrepHead"><span>REPLAY SYMBOLS</span><b id="wsPrepSummary">0 / 0 ACTIVE</b></div><div id="wsPrepDay" class="wsPrepDay">等待选择股票</div><div id="wsPrepList" class="wsPrepList"></div><div id="wsPrepNote" class="wsPrepNote"></div>`;
const universe=document.getElementById('universe');
if(universe?.parentElement)universe.parentElement.appendChild(prepPane);

function ensureMetrics(){
 let host=document.getElementById('wsMetrics');if(host){const center=$('.center');if(center)center.style.gridTemplateRows='38px auto minmax(0,1fr)';return host}
 host=el('div','wsMetrics');host.id='wsMetrics';host.innerHTML=`<div class="wsMetric"><span>Window Delta</span><b id="wmDelta">—</b></div><div class="wsMetric"><span>Price Move</span><b id="wmMove">—</b></div><div class="wsMetric"><span>Agg Buy</span><b id="wmBuy">—</b></div><div class="wsMetric"><span>Agg Sell</span><b id="wmSell">—</b></div>`;
 const center=$('.center');const viewBar=$('.viewBar');if(center&&viewBar){center.insertBefore(host,viewBar.nextSibling);center.style.gridTemplateRows='38px auto minmax(0,1fr)'}else if(center)center.prepend(host);return host;
}
function renderMetrics(){
 ensureMetrics();const m=selectedMetrics();if(!m)return;
 const d=+m.delta||0,move=+m.move_bps||0,buy=+m.buy_volume||0,sell=+m.sell_volume||0;
 const set=(id,text,cls)=>{const n=document.getElementById(id);if(!n)return;n.textContent=text;n.className=cls||''};
 set('wmDelta',fmt(d),d>=0?'up':'dn');set('wmMove',bps(move),move>=0?'up':'dn');set('wmBuy',fmt(buy),'up');set('wmSell',fmt(sell),'dn');
}
function marketClock(ns){if(!ns)return'--:--:-- ET';return new Date(Number(ns)/1e6).toLocaleString('en-US',{timeZone:'America/New_York',month:'short',day:'2-digit',hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'})+' ET'}
function marketPrice(){
 const s=selectedSymbol();if(!s)return'—';
 const t=snapshot?.trades?.[s]||window.OF?.latest?.(s,'trade');
 const q=snapshot?.quotes?.[s]||window.OF?.latest?.(s,'quote');
 let p=Number(t?.price);if(!(Number.isFinite(p)&&p>0))p=Number(q?.last);
 if(!(Number.isFinite(p)&&p>0)){const bid=Number(q?.bid),ask=Number(q?.ask);if(bid>0&&ask>0)p=(bid+ask)/2}
 return Number.isFinite(p)&&p>0?`${s}  $${p.toFixed(2)}`:`${s}  —`;
}
function renderReplay(){
 const isReplay=isReplayMode();document.getElementById('wsMode').textContent=isReplay?'REPLAY':'LIVE';bar.classList.toggle('on',isReplay);
 document.getElementById('wsReplayBtn').classList.toggle('on',isReplay);
 if(!isReplay)return;replay=snapshot?.replay||snapshot?.status?.replay||replay;if(!replay)return;
 const progress=Math.max(0,Math.min(1,Number(replay.progress||0)));document.getElementById('wsSeek').value=Math.round(progress*1000);
 const pct=Math.max(4,Math.min(96,progress*100));const clock=document.getElementById('wsClock'),price=document.getElementById('wsPrice');clock.textContent=marketClock(replay.market_time_ns);price.textContent=marketPrice();clock.style.left=`${pct}%`;price.style.left=`${pct}%`;
 document.getElementById('wsPlay').textContent=replay.playing?'Ⅱ PAUSE':'▶ PLAY';document.getElementById('wsSpeed').value=String(replay.speed||1);
}
async function control(action,extra={}){const r=await fetch('/api/replay/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,...extra})});if(!r.ok){alert(await r.text());return}replay=await r.json();if(snapshot)snapshot.replay=replay;renderReplay()}
bar.querySelectorAll('button[data-a]').forEach(b=>b.onclick=()=>{let a=b.dataset.a;if(a==='play'&&replay?.playing)a='pause';control(a)});
document.getElementById('wsSeek').onchange=e=>control('seek',{progress:+e.target.value/1000});document.getElementById('wsSpeed').onchange=e=>control('speed',{speed:+e.target.value});
document.getElementById('wsNoviceBtn').onclick=()=>{novice.classList.toggle('on');document.getElementById('wsNoviceBtn').classList.toggle('on',novice.classList.contains('on'))};

function prepSymbols(s){
 const values=s?.all_symbols||s?.symbols||snapshot?.status?.symbols||[];
 return [...new Set(values.map(x=>String(x).toUpperCase()))];
}
function prepReadySet(s,syms){
 const ready=s?.ready_symbols||s?.active_symbols||[...(s?.cached_symbols||[]),...(s?.downloaded_symbols||[])];
 return new Set(ready.map(x=>String(x).toUpperCase()));
}
function renderPrepUniverse(s=prepareInfo){
 const btn=document.getElementById('wsReplayBtn');const hint=document.getElementById('uHint');
 if(!s||s.state==='idle'){
   prepPane.classList.remove('on');btn.classList.remove('preparing');btn.textContent='SIM REPLAY';return;
 }
 const syms=prepSymbols(s);if(!syms.length){prepPane.classList.remove('on');return}
 const ready=prepReadySet(s,syms);const current=String(s.current_symbol||'').toUpperCase();const failed=s.state==='error'?current:'';
 prepPane.classList.add('on');if(hint)hint.textContent=s.state==='ready'?'REPLAY ACTIVE':'REPLAY PREP';
 const activeCount=syms.filter(x=>ready.has(x)).length;document.getElementById('wsPrepSummary').textContent=`${activeCount} / ${syms.length} ACTIVE`;
 document.getElementById('wsPrepDay').textContent=s.trading_day?`${s.trading_day} · ALPACA SIP · REGULAR SESSION`:'正在确认最近完整交易日…';
 document.getElementById('wsPrepList').innerHTML=syms.map((sym,i)=>{
   const isReady=ready.has(sym),isCurrent=current===sym&&!isReady,isError=failed===sym;
   const cls=isError?'error':isReady?'active':'inactive';
   const label=isError?'ERROR':isReady?'ACTIVE':isCurrent?'INACTIVE · DOWNLOADING':'INACTIVE · QUEUED';
   const marker=isReady?'●':isError?'×':isCurrent?'◌':'○';
   return `<button class="wsPrepRow ${cls}" data-prep-sym="${sym}" ${isReady&&isReplayMode()?'':'disabled'}><span class="wsPrepDot">${marker}</span><b>${sym}</b><span class="wsPrepIndex">${i+1}/${syms.length}</span><span class="wsPrepState">${label}</span></button>`;
 }).join('');
 document.querySelectorAll('.wsPrepRow.active:not(:disabled)').forEach(row=>row.onclick=()=>window.OF?.selectSymbol?.(row.dataset.prepSym));
 const note=document.getElementById('wsPrepNote');
 if(s.state==='error')note.textContent=s.message||'下载失败。点击 SIM REPLAY 可重新设置。';
 else if(s.state==='ready'||isReplayMode())note.textContent='全部录像已准备完成。ACTIVE symbol 可直接切换查看。';
 else note.textContent=`后台逐只下载；当前 ${s.symbol_index||Math.min(activeCount+1,syms.length)} / ${s.symbol_total||syms.length}。主界面不会被锁定。`;
 const preparing=s.state==='preparing';btn.classList.toggle('preparing',preparing);btn.textContent=preparing?`REPLAY ${activeCount}/${syms.length}`:'SIM REPLAY';
}

function openReplayModal(){
 document.getElementById('wsReplayError').textContent='';modal.classList.add('on');
 fetch('/api/replay/prepare',{cache:'no-store'}).then(r=>r.json()).then(info=>{
   prepareInfo=info;const cap=Number(info.symbol_cap||5);document.getElementById('wsReplayHint').textContent=`最多 ${cap} 只；逐只完整下载，完成后左侧变为 ACTIVE。`;
   const current=(info.current_symbols||snapshot?.status?.symbols||[]).slice(0,cap);if(!document.getElementById('wsReplaySymbols').value)document.getElementById('wsReplaySymbols').value=current.join(' ');
   document.getElementById('wsReplayCredentials').style.display=info.credentials_configured?'none':'block';renderPrepUniverse(info);
 }).catch(()=>{});
}
document.getElementById('wsReplayBtn').onclick=()=>{if(isReplayMode()){bar.classList.toggle('on');return}if(prepareInfo?.state==='preparing'){prepPane.classList.add('on');return}openReplayModal()};
document.getElementById('wsReplayClose').onclick=()=>modal.classList.remove('on');

function stopPreparePoll(){if(preparePoll){clearInterval(preparePoll);preparePoll=null}}
async function pollPrepare(){
 try{const r=await fetch('/api/replay/prepare',{cache:'no-store'});if(!r.ok)return;const s=await r.json();prepareInfo=s;renderPrepUniverse(s);
   if(s.state==='preparing')return;
   if(s.state==='ready'){stopPreparePoll();await refresh();renderPrepUniverse(s);return}
   if(s.state==='error'){stopPreparePoll();renderPrepUniverse(s);return}
 }catch(e){}
}
function beginPreparePoll(){stopPreparePoll();preparePoll=setInterval(pollPrepare,700);pollPrepare()}
document.getElementById('wsReplayStart').onclick=async()=>{
 const symbols=splitSymbols(document.getElementById('wsReplaySymbols').value);const cap=Number(prepareInfo?.symbol_cap||5);const err=document.getElementById('wsReplayError');err.textContent='';
 if(!symbols.length){err.textContent='请输入至少一只股票。';return}if(symbols.length>cap){err.textContent=`最多选择 ${cap} 只股票。`;return}
 const payload={symbols};if(!prepareInfo?.credentials_configured){payload.api_key=document.getElementById('wsReplayKey').value.trim();payload.api_secret=document.getElementById('wsReplaySecret').value.trim()}
 try{
   const r=await fetch('/api/replay/prepare',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
   if(!r.ok){err.textContent=await r.text();return}
   prepareInfo=await r.json();modal.classList.remove('on');renderPrepUniverse(prepareInfo);beginPreparePoll();
 }catch(e){err.textContent=String(e)}
};

async function refresh(){try{const r=await fetch('/api/snapshot',{cache:'no-store'});if(r.ok){snapshot=await r.json();renderMetrics();renderReplay();renderPrepUniverse(prepareInfo)}}catch(e){}}
setInterval(refresh,750);setTimeout(refresh,50);setTimeout(ensureMetrics,100);
setTimeout(async()=>{try{const r=await fetch('/api/replay/prepare',{cache:'no-store'});if(r.ok){const s=await r.json();prepareInfo=s;renderPrepUniverse(s);if(s.state==='preparing')beginPreparePoll()}}catch(e){}},150);

const old=window.WebSocket;if(old&&!window.__wsReplayWrapped){window.__wsReplayWrapped=true}
})();
