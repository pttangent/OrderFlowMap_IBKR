(()=>{
const O=window.OF,$=O.$;
O.showTab=name=>{document.querySelectorAll('.tab').forEach(b=>b.classList.toggle('on',b.dataset.tab===name));['tape','footprint','profile'].forEach(id=>$(id).style.display=id===name?'block':'none')};
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>O.showTab(b.dataset.tab));
$('bubbleMax').oninput=e=>{O.C.bubbleMax=+e.target.value;$('bubbleMaxV').textContent=e.target.value;O.drawOverlay?.()};
$('minTrade').oninput=e=>{O.C.minTrade=+e.target.value;$('minTradeV').textContent=e.target.value;O.drawOverlay?.()};
$('largeQty').oninput=e=>{O.C.largeQty=+e.target.value;$('largeQtyV').textContent=e.target.value;O.drawOverlay?.()};
$('showBubbles').onchange=e=>{O.C.showBubbles=e.target.checked;O.drawOverlay?.()};
$('showTrail').onchange=e=>{O.C.showTrail=e.target.checked;O.drawOverlay?.()};
$('showBbo').onchange=e=>{O.C.showBbo=e.target.checked;O.renderSeries?.()};
$('showVwap').onchange=e=>{O.C.showVwap=e.target.checked;O.renderSeries?.()};
$('historySec').onchange=e=>{O.C.historySec=+e.target.value;if(O.S.selected){O.S.loaded[O.S.selected]=false;O.loadHistory(O.S.selected,true)}};
$('fitBtn').onclick=()=>O.fitCharts?.();
$('helpBtn').onclick=()=>$('helpModal').classList.add('on');
$('helpClose').onclick=()=>$('helpModal').classList.remove('on');
$('helpModal').onclick=e=>{if(e.target===$('helpModal'))$('helpModal').classList.remove('on')};
document.addEventListener('keydown',e=>{if(e.key==='Escape')$('helpModal').classList.remove('on');if(e.key==='f'||e.key==='F')O.fitCharts?.()});
(async()=>{try{const d=await fetch('/api/snapshot',{cache:'no-store'}).then(r=>r.json());O.applySnapshot(d)}catch(e){console.error(e);$('status').textContent='snapshot error';$('dot').className='dot err'}O.connectWS();O.resizeCanvas?.()})();
})();
