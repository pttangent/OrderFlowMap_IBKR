(()=>{
const $=s=>document.querySelector(s);
const el=(tag,cls,html='')=>{const n=document.createElement(tag);n.className=cls;n.innerHTML=html;return n};
const fmt=n=>Number(n||0).toLocaleString(undefined,{maximumFractionDigits:0});
const bps=n=>`${Number(n||0)>=0?'+':''}${Number(n||0).toFixed(2)} bps`;
let snapshot=null,replay=null,prepareInfo=null,preparePoll=null;

function selectedSymbol(){return window.OF?.S?.selected||snapshot?.status?.symbols?.[0]||''}
function selectedMetrics(){const s=selectedSymbol();return snapshot?.metrics?.[s]||window.OF?.latest?.(s,'metrics')||null}
function splitSymbols(text){return [...new Set(String(text||'').toUpperCase().split(/[\s,]+/).map(x=>x.trim()).filter(Boolean))]}

const top=el('div','wsTopActions');
top.innerHTML=`<span class="wsMode">MODE <b id="wsMode">...</b></span><a class="wsAction" href="/radar">RADAR</a><button class="wsAction" id="wsNoviceBtn">NOVICE</button><a class="wsAction" href="/learn" target="_blank">TEACHING</a><button class="wsAction replay" id="wsReplayBtn">SIM REPLAY</button>`;
document.body.appendChild(top);

const bar=el('div','wsReplayBar');
bar.innerHTML=`<button data-a="restart">⏮</button><button data-a="prev_bar">◀ BAR</button><button data-a="play" id="wsPlay">▶ PLAY</button><button data-a="next_bar">BAR ▶</button><div class="wsTimeline"><span class="wsTimelineTop" id="wsClock">--:--:-- ET</span><input id="wsSeek" type="range" min="0" max="1000" value="0"><span class="wsTimelineBottom" id="wsPrice">—</span></div><select id="wsSpeed"><option>.25</option><option>.5</option><option selected>1</option><option>2</option><option>5</option><option>10</option><option>25</option><option>100</option></select>`;
document.body.appendChild(bar);

const novice=el('aside','wsNovice');
novice.innerHTML=`<h3>NOVICE // 当前 View 怎么看</h3><h4>CANDLE + FOOTPRINT 回答什么？</h4><ul><li><strong>哪几个价位发生最大交换？</strong> 看每根 Bar 的 POC 与 Volume。</li><li><strong>哪边主动成交占优？</strong> 看 Sell@Bid / Buy@Ask / Δ。</li><li><strong>单边成交是否真的推动价格？</strong> 把 Δ 和左边 Candle 的实际价格结果一起看。</li></ul><h4>最重要的判断顺序</h4><p>① 先看 Candle 有没有价格进展。<br>② 再看同一根 Footprint 谁在主动成交。<br>③ 如果成交方向和价格结果背离，切到 Pressure 检查 Absorption。<br>④ CVD 用于确认/背离，不单独作为方向信号。</p><h4>结构标记</h4><p><strong>青框：</strong>单根 Bar POC。<br><strong>橙框：</strong>Diagonal Imbalance。<br><strong>BID ABS：</strong>负 Δ 但 Candle 上涨。<br><strong>OFFER ABS：</strong>正 Δ 但 Candle 下跌。</p><h4>Replay</h4><p>SIM REPLAY 会先用 Alpaca 免费 Historical SIP 下载最近一个完整交易日的逐笔成交与 BBO context，再热切换整个 Workstation。Replay 期间所有页面跟随同一市场时钟。</p>`;
document.body.appendChild(novice);

const modal=el('div','wsReplayModal');
modal.innerHTML=`<div class="wsReplayDialog"><button class="wsReplayClose" id="wsReplayClose">×</button><div class="wsReplayKicker">GLOBAL MARKET REPLAY</div><h2>载入最近完整交易日</h2><p class="wsReplayLead">输入要关注的美股。系统会使用 Alpaca Historical SIP 下载上一完整交易日的 trades + quotes，完成后整个 Workstation 自动切换到 Replay。</p><label>Symbols</label><input id="wsReplaySymbols" class="wsReplayInput" autocomplete="off" spellcheck="false" placeholder="XE SNDK NVDA"><div class="wsReplayHint" id="wsReplayHint">免费层安全上限：30 symbols</div><div id="wsReplayCredentials"><label>Alpaca API Key</label><input id="wsReplayKey" class="wsReplayInput" type="password" autocomplete="off"><label>Alpaca API Secret</label><input id="wsReplaySecret" class="wsReplayInput" type="password" autocomplete="off"><div class="wsReplayHint">仅发送到本机 Workstation，本页不保存凭证。</div></div><div class="wsReplayError" id="wsReplayError"></div><button class="wsReplayStart" id="wsReplayStart">LOAD PREVIOUS SESSION</button></div>`;
document.body.appendChild(modal);

const loader=el('div','wsReplayLoading');
loader.innerHTML=`<div class="wsLoaderCard"><div class="wsSpinner"></div><div class="wsLoaderKicker">PREPARING REPLAY</div><h2 id="wsLoaderTitle">正在准备市场录像</h2><p id="wsLoaderDetail">正在连接 Alpaca Historical SIP…</p><div class="wsLoaderStats" id="wsLoaderStats"></div><div class="wsLoaderLock">下载完成前已锁定操作。LIVE 数据仍在后台运行，直到 Replay 可以安全接管。</div></div>`;
document.body.appendChild(loader);

function ensureMetrics(){
 let host=document.getElementById('wsMetrics');if(host)return host;
 host=el('div','wsMetrics');host.id='wsMetrics';host.innerHTML=`<div class="wsMetric"><span>Window Delta</span><b id="wmDelta">—</b></div><div class="wsMetric"><span>Price Move</span><b id="wmMove">—</b></div><div class="wsMetric"><span>Agg Buy</span><b id="wmBuy">—</b></div><div class="wsMetric"><span>Agg Sell</span><b id="wmSell">—</b></div>`;
 const center=$('.center');const viewBar=$('.viewBar');if(center&&viewBar)center.insertBefore(host,viewBar.nextSibling);else if(center)center.prepend(host);return host;
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
 let p=Number(t?.price);
 if(!Number.isFinite(p))p=Number(q?.last);
 if(!Number.isFinite(p)){const bid=Number(q?.bid),ask=Number(q?.ask);if(Number.isFinite(bid)&&Number.isFinite(ask))p=(bid+ask)/2}
 return Number.isFinite(p)?`${s}  $${p.toFixed(2)}`:`${s}  —`;
}
function renderReplay(){
 const mode=snapshot?.status?.runtime_mode||snapshot?.status?.mode||'LIVE';const isReplay=mode==='REPLAY';
 document.getElementById('wsMode').textContent=isReplay?'REPLAY':'LIVE';document.getElementById('wsReplayBtn').classList.toggle('on',isReplay);bar.classList.toggle('on',isReplay);
 if(!isReplay)return;replay=snapshot?.replay||snapshot?.status?.replay||replay;if(!replay)return;
 const progress=Math.max(0,Math.min(1,Number(replay.progress||0)));document.getElementById('wsSeek').value=Math.round(progress*1000);
 const pct=Math.max(4,Math.min(96,progress*100));const clock=document.getElementById('wsClock'),price=document.getElementById('wsPrice');clock.textContent=marketClock(replay.market_time_ns);price.textContent=marketPrice();clock.style.left=`${pct}%`;price.style.left=`${pct}%`;
 document.getElementById('wsPlay').textContent=replay.playing?'Ⅱ PAUSE':'▶ PLAY';document.getElementById('wsSpeed').value=String(replay.speed||1);
}
async function control(action,extra={}){const r=await fetch('/api/replay/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,...extra})});if(!r.ok){alert(await r.text());return}replay=await r.json();if(snapshot)snapshot.replay=replay;renderReplay()}
bar.querySelectorAll('button[data-a]').forEach(b=>b.onclick=()=>{let a=b.dataset.a;if(a==='play'&&replay?.playing)a='pause';control(a)});
document.getElementById('wsSeek').onchange=e=>control('seek',{progress:+e.target.value/1000});document.getElementById('wsSpeed').onchange=e=>control('speed',{speed:+e.target.value});
document.getElementById('wsNoviceBtn').onclick=()=>{novice.classList.toggle('on');document.getElementById('wsNoviceBtn').classList.toggle('on',novice.classList.contains('on'))};

function openReplayModal(){
 document.getElementById('wsReplayError').textContent='';modal.classList.add('on');
 fetch('/api/replay/prepare',{cache:'no-store'}).then(r=>r.json()).then(info=>{
   prepareInfo=info;const cap=Number(info.symbol_cap||30);document.getElementById('wsReplayHint').textContent=`最多 ${cap} 只；免费 Historical SIP 为 200 req/min，完整逐笔越多下载越久。`;
   const current=(info.current_symbols||snapshot?.status?.symbols||[]).slice(0,cap);if(!document.getElementById('wsReplaySymbols').value)document.getElementById('wsReplaySymbols').value=current.join(' ');
   document.getElementById('wsReplayCredentials').style.display=info.credentials_configured?'none':'block';
 }).catch(()=>{});
}
document.getElementById('wsReplayBtn').onclick=()=>{const isReplay=(snapshot?.status?.runtime_mode||snapshot?.status?.mode)==='REPLAY';if(isReplay){bar.classList.toggle('on');return}openReplayModal()};
document.getElementById('wsReplayClose').onclick=()=>modal.classList.remove('on');

function stageText(s){return({queued:'排入下载队列',resolving_day:'确认最近完整美股交易日',downloading:'下载整日 trades + quotes',indexing:'建立本地 Replay tape',switching:'切换全局 Runtime',ready:'Replay 已就绪',error:'Replay 准备失败'})[s]||'正在准备市场录像'}
function showLoader(state){
 loader.classList.add('on');document.getElementById('wsLoaderTitle').textContent=stageText(state.stage);const day=state.trading_day?`交易日 ${state.trading_day} · `:'';document.getElementById('wsLoaderDetail').textContent=`${day}${(state.symbols||[]).join(' · ')}`;
 const stats=[];if(state.trades!=null)stats.push(`Trades ${fmt(state.trades)}`);if(state.quotes!=null)stats.push(`Quotes ${fmt(state.quotes)}`);if(state.requests!=null)stats.push(`API ${fmt(state.requests)}`);document.getElementById('wsLoaderStats').textContent=stats.join('  /  ')||'Alpaca SIP historical · local SQLite';
}
function stopPreparePoll(){if(preparePoll){clearInterval(preparePoll);preparePoll=null}}
async function pollPrepare(){
 try{const r=await fetch('/api/replay/prepare',{cache:'no-store'});if(!r.ok)return;const s=await r.json();prepareInfo=s;
   if(s.state==='preparing'){showLoader(s);return}
   if(s.state==='ready'){stopPreparePoll();loader.classList.remove('on');modal.classList.remove('on');await refresh();return}
   if(s.state==='error'){stopPreparePoll();loader.classList.remove('on');modal.classList.add('on');document.getElementById('wsReplayError').textContent=s.message||'Replay preparation failed';return}
 }catch(e){}
}
function beginPreparePoll(){stopPreparePoll();preparePoll=setInterval(pollPrepare,700);pollPrepare()}
document.getElementById('wsReplayStart').onclick=async()=>{
 const symbols=splitSymbols(document.getElementById('wsReplaySymbols').value);const cap=Number(prepareInfo?.symbol_cap||30);const err=document.getElementById('wsReplayError');err.textContent='';
 if(!symbols.length){err.textContent='请输入至少一只股票。';return}if(symbols.length>cap){err.textContent=`最多选择 ${cap} 只股票。`;return}
 const payload={symbols};if(!prepareInfo?.credentials_configured){payload.api_key=document.getElementById('wsReplayKey').value.trim();payload.api_secret=document.getElementById('wsReplaySecret').value.trim()}
 showLoader({stage:'queued',symbols});
 try{const r=await fetch('/api/replay/prepare',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});if(!r.ok){loader.classList.remove('on');err.textContent=await r.text();return}beginPreparePoll()}catch(e){loader.classList.remove('on');err.textContent=String(e)}
};

async function refresh(){try{const r=await fetch('/api/snapshot',{cache:'no-store'});if(r.ok){snapshot=await r.json();renderMetrics();renderReplay()}}catch(e){}}
setInterval(refresh,750);setTimeout(refresh,50);setTimeout(ensureMetrics,100);
setTimeout(async()=>{try{const r=await fetch('/api/replay/prepare',{cache:'no-store'});if(r.ok){const s=await r.json();prepareInfo=s;if(s.state==='preparing')beginPreparePoll()}}catch(e){}},150);

const old=window.WebSocket;if(old&&!window.__wsReplayWrapped){window.__wsReplayWrapped=true}
})();
