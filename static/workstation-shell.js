(()=>{
const $=s=>document.querySelector(s);
const el=(tag,cls,html='')=>{const n=document.createElement(tag);n.className=cls;n.innerHTML=html;return n};
const fmt=n=>Number(n||0).toLocaleString(undefined,{maximumFractionDigits:0});
const bps=n=>`${Number(n||0)>=0?'+':''}${Number(n||0).toFixed(2)} bps`;
let snapshot=null,replay=null;

function selectedSymbol(){return window.OF?.S?.selected||snapshot?.status?.symbols?.[0]||''}
function selectedMetrics(){const s=selectedSymbol();return snapshot?.metrics?.[s]||window.OF?.latest?.(s,'metrics')||null}

const top=el('div','wsTopActions');
top.innerHTML=`<span class="wsMode">MODE <b id="wsMode">...</b></span><a class="wsAction" href="/radar">RADAR</a><button class="wsAction" id="wsNoviceBtn">NOVICE</button><a class="wsAction" href="/learn" target="_blank">TEACHING</a><button class="wsAction replay" id="wsReplayBtn">SIM REPLAY</button>`;
document.body.appendChild(top);

const bar=el('div','wsReplayBar');
bar.innerHTML=`<button data-a="restart">⏮</button><button data-a="prev_bar">◀ BAR</button><button data-a="play" id="wsPlay">▶ PLAY</button><button data-a="next_bar">BAR ▶</button><input id="wsSeek" type="range" min="0" max="1000" value="0"><select id="wsSpeed"><option>.25</option><option>.5</option><option selected>1</option><option>2</option><option>5</option><option>10</option><option>25</option><option>100</option></select><span class="wsReplayClock" id="wsClock">--:--:--</span>`;
document.body.appendChild(bar);

const novice=el('aside','wsNovice');
novice.innerHTML=`<h3>NOVICE // 当前 View 怎么看</h3><h4>CANDLE + FOOTPRINT 回答什么？</h4><ul><li><strong>哪几个价位发生最大交换？</strong> 看每根 Bar 的 POC 与 Volume。</li><li><strong>哪边主动成交占优？</strong> 看 Sell@Bid / Buy@Ask / Δ。</li><li><strong>单边成交是否真的推动价格？</strong> 把 Δ 和左边 Candle 的实际价格结果一起看。</li></ul><h4>最重要的判断顺序</h4><p>① 先看 Candle 有没有价格进展。<br>② 再看同一根 Footprint 谁在主动成交。<br>③ 如果成交方向和价格结果背离，切到 Pressure 检查 Absorption。<br>④ CVD 用于确认/背离，不单独作为方向信号。</p><h4>结构标记</h4><p><strong>青框：</strong>单根 Bar POC。<br><strong>橙框：</strong>Diagonal Imbalance。<br><strong>BID ABS：</strong>负 Δ 但 Candle 上涨。<br><strong>OFFER ABS：</strong>正 Δ 但 Candle 下跌。</p><h4>Replay</h4><p>SIM REPLAY 是整个 Workstation 的数据源模式，不是一个分页。进入后 Radar、Footprint、Pressure、CVD 都跟随同一个市场时钟。</p>`;
document.body.appendChild(novice);

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
function marketClock(ns){if(!ns)return'--:--:--';return new Date(Number(ns)/1e6).toLocaleTimeString('en-US',{timeZone:'America/New_York',hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'})+' ET'}
function renderReplay(){
 const mode=snapshot?.status?.runtime_mode||snapshot?.status?.mode||'LIVE';const isReplay=mode==='REPLAY';
 document.getElementById('wsMode').textContent=isReplay?'REPLAY':'LIVE';document.getElementById('wsReplayBtn').classList.toggle('on',isReplay);bar.classList.toggle('on',isReplay);
 if(!isReplay)return;replay=snapshot?.replay||snapshot?.status?.replay||replay;if(!replay)return;
 document.getElementById('wsSeek').value=Math.round((replay.progress||0)*1000);document.getElementById('wsClock').textContent=marketClock(replay.market_time_ns);document.getElementById('wsPlay').textContent=replay.playing?'Ⅱ PAUSE':'▶ PLAY';document.getElementById('wsSpeed').value=String(replay.speed||1);
}
async function control(action,extra={}){const r=await fetch('/api/replay/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,...extra})});if(!r.ok){alert(await r.text());return}replay=await r.json();if(snapshot)snapshot.replay=replay;renderReplay()}
bar.querySelectorAll('button[data-a]').forEach(b=>b.onclick=()=>{let a=b.dataset.a;if(a==='play'&&replay?.playing)a='pause';control(a)});
document.getElementById('wsSeek').onchange=e=>control('seek',{progress:+e.target.value/1000});document.getElementById('wsSpeed').onchange=e=>control('speed',{speed:+e.target.value});
document.getElementById('wsNoviceBtn').onclick=()=>{novice.classList.toggle('on');document.getElementById('wsNoviceBtn').classList.toggle('on',novice.classList.contains('on'))};
document.getElementById('wsReplayBtn').onclick=()=>{const isReplay=(snapshot?.status?.runtime_mode||snapshot?.status?.mode)==='REPLAY';if(isReplay){bar.classList.toggle('on');return}alert('当前服务是 LIVE。全局 Replay 需要以 --source replay 启动 Workstation；Replay 数据源会以 SQLite mode=ro 打开，不写回原始录影。')};

async function refresh(){try{const r=await fetch('/api/snapshot',{cache:'no-store'});if(r.ok){snapshot=await r.json();renderMetrics();renderReplay()}}catch(e){}}
setInterval(refresh,750);setTimeout(refresh,50);setTimeout(ensureMetrics,100);

// WebSocket replay state updates should refresh the global control immediately.
const old=window.WebSocket;if(old&&!window.__wsReplayWrapped){window.__wsReplayWrapped=true}
})();
