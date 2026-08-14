(()=>{
const OF=window.OF,$=OF.$,LC=window.LightweightCharts;
const chartOpts={autoSize:true,layout:{background:{type:'solid',color:'#090909'},textColor:'#888'},grid:{vertLines:{color:'#171717'},horzLines:{color:'#171717'}},rightPriceScale:{borderColor:'#252525'},timeScale:{borderColor:'#252525',timeVisible:true,secondsVisible:true,rightOffset:4},crosshair:{vertLine:{color:'#444'},horzLine:{color:'#444'}}};
OF.pc=LC.createChart($('priceChart'),chartOpts);
OF.priceSeries=OF.pc.addSeries(LC.LineSeries,{color:'#d7d7d7',lineWidth:1,priceLineVisible:false,lastValueVisible:true});
OF.bidSeries=OF.pc.addSeries(LC.LineSeries,{color:'#4bd39b',lineWidth:1,priceLineVisible:false,lastValueVisible:false});
OF.askSeries=OF.pc.addSeries(LC.LineSeries,{color:'#ff7474',lineWidth:1,priceLineVisible:false,lastValueVisible:false});
OF.vwapSeries=OF.pc.addSeries(LC.LineSeries,{color:'#e6bd61',lineWidth:1,priceLineVisible:false,lastValueVisible:false});
OF.cc=LC.createChart($('cvdChart'),{...chartOpts,timeScale:{...chartOpts.timeScale,visible:false}});
OF.cvdSeries=OF.cc.addSeries(LC.LineSeries,{color:'#66c7e8',lineWidth:2,priceLineVisible:false,lastValueVisible:true});
OF.validPrice=x=>Number.isFinite(Number(x))&&Number(x)>0;
OF.coalesce=(rows,getValue)=>{const m=new Map();for(const r of rows){const v=getValue(r);if(!OF.finite(v))continue;const t=OF.sec(r.ts_ns);m.set(t,{time:t,value:Number(v)})}return[...m.values()].sort((a,b)=>a.time-b.time)};
OF.priceCoalesce=(rows,getValue)=>OF.coalesce(rows,getValue).filter(x=>OF.validPrice(x.value));
OF.pricePoints=sym=>{const e=[];for(const q of OF.S.quotes[sym]||[])if(OF.validPrice(q.last))e.push({ts_ns:q.ts_ns,p:q.last});for(const t of OF.S.trades[sym]||[])if(OF.validPrice(t.price))e.push({ts_ns:t.ts_ns,p:t.price});e.sort((a,b)=>Number(a.ts_ns)-Number(b.ts_ns));return OF.priceCoalesce(e,x=>x.p)};
OF.renderSeries=()=>{const sym=OF.S.selected;if(!sym)return;OF.ensure(sym);OF.priceSeries.setData(OF.pricePoints(sym));OF.bidSeries.setData(OF.C.showBbo?OF.priceCoalesce(OF.S.quotes[sym],x=>x.bid):[]);OF.askSeries.setData(OF.C.showBbo?OF.priceCoalesce(OF.S.quotes[sym],x=>x.ask):[]);OF.vwapSeries.setData(OF.C.showVwap?OF.priceCoalesce(OF.S.quotes[sym],x=>x.vwap):[]);OF.cvdSeries.setData(OF.coalesce(OF.S.metrics[sym],x=>x.cvd));OF.drawOverlay()};
OF.fitCharts=()=>{OF.pc.timeScale().fitContent();OF.cc.timeScale().fitContent();OF.drawOverlay()};

const zoomStyle=document.createElement('style');
zoomStyle.textContent='.ofCanvasZoom{position:absolute;top:9px;right:10px;z-index:7;display:flex;gap:3px}.ofCanvasZoom button{width:25px;height:25px;padding:0;border:1px solid #394b52;background:rgba(9,9,9,.9);color:#c8e7ee;font:700 16px ui-monospace;line-height:22px;cursor:pointer}.ofCanvasZoom button:hover{background:#183039;color:#fff}.ofCanvasZoom button:active{background:#24505d}#gov{right:70px!important}';
document.head.appendChild(zoomStyle);
const zoom=document.createElement('div');
zoom.className='ofCanvasZoom';
zoom.innerHTML='<button type="button" title="Zoom out canvas">−</button><button type="button" title="Zoom in canvas">+</button>';
$("chartwrap").appendChild(zoom);
const zoomCanvas=factor=>{const scale=OF.pc.timeScale(),range=scale.getVisibleLogicalRange();if(!range)return;const center=(range.from+range.to)/2,span=Math.max(10,(range.to-range.from)*factor);scale.setVisibleLogicalRange({from:center-span/2,to:center+span/2});OF.drawOverlay()};
zoom.querySelector('button:first-child').onclick=()=>zoomCanvas(1.25);
zoom.querySelector('button:last-child').onclick=()=>zoomCanvas(.8);
const formationReset=document.createElement('button');formationReset.type='button';formationReset.title='Reset Formation to latest';formationReset.textContent='↻';zoom.appendChild(formationReset);
OF.resetFormationView=()=>{OF.pc.timeScale().scrollToRealTime();OF.cc.timeScale().scrollToRealTime();OF.drawOverlay()};
formationReset.onclick=()=>OF.resetFormationView();

OF.resizeCanvas=()=>{const el=$('chartwrap'),cv=$('overlay'),r=el.getBoundingClientRect(),dpr=window.devicePixelRatio||1;cv.width=Math.max(1,Math.floor(r.width*dpr));cv.height=Math.max(1,Math.floor(r.height*dpr));cv.style.width=r.width+'px';cv.style.height=r.height+'px';OF.drawOverlay()};
const p95=a=>{if(!a.length)return 1;const b=[...a].sort((x,y)=>x-y);return b[Math.floor((b.length-1)*.95)]||1};
OF.drawOverlay=()=>{const sym=OF.S.selected,cv=$('overlay');if(!sym||!cv.width)return;const dpr=window.devicePixelRatio||1,ctx=cv.getContext('2d'),w=cv.width/dpr,h=cv.height/dpr;ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);const quotes=OF.S.quotes[sym]||[],trades=OF.S.trades[sym]||[];
 if(OF.C.showTrail&&quotes.length){const qs=[];let lastSec=-1;for(const q of quotes.slice(-1800)){const s=OF.sec(q.ts_ns);if(s===lastSec)qs[qs.length-1]=q;else{qs.push(q);lastSec=s}}const sizes=[];for(const q of qs){if(OF.finite(q.bid_size))sizes.push(Number(q.bid_size));if(OF.finite(q.ask_size))sizes.push(Number(q.ask_size))}const ref=p95(sizes);for(let i=0;i<qs.length;i++){const q=qs[i],x=OF.pc.timeScale().timeToCoordinate(OF.sec(q.ts_ns));if(x==null)continue;const n=i+1<qs.length?OF.pc.timeScale().timeToCoordinate(OF.sec(qs[i+1].ts_ns)):x+4;if(OF.C.showBbo&&OF.validPrice(q.bid)){const y=OF.priceSeries.priceToCoordinate(Number(q.bid));if(y!=null){const k=Math.min(1,Math.sqrt((Number(q.bid_size)||0)/ref));ctx.strokeStyle=`rgba(75,211,155,${.05+.22*k})`;ctx.lineWidth=1+2*k;ctx.beginPath();ctx.moveTo(x,y);ctx.lineTo(n==null?x+4:n,y);ctx.stroke()}}if(OF.C.showBbo&&OF.validPrice(q.ask)){const y=OF.priceSeries.priceToCoordinate(Number(q.ask));if(y!=null){const k=Math.min(1,Math.sqrt((Number(q.ask_size)||0)/ref));ctx.strokeStyle=`rgba(255,116,116,${.05+.22*k})`;ctx.lineWidth=1+2*k;ctx.beginPath();ctx.moveTo(x,y);ctx.lineTo(n==null?x+4:n,y);ctx.stroke()}}}}
 if(!OF.C.showBubbles||!trades.length)return;const groups=new Map();for(const t of trades.slice(-3500)){if(!OF.validPrice(t.price)||!OF.finite(t.size)||Number(t.size)<OF.C.minTrade)continue;const s=OF.sec(t.ts_ns),side=t.aggressor||'UNKNOWN',key=s+'|'+Number(t.price).toFixed(4)+'|'+side,g=groups.get(key)||{time:s,price:Number(t.price),size:0,side,proxy:String(t.quality||'').includes('PROXY')};g.size+=Number(t.size);groups.set(key,g)}
 for(const g of groups.values()){const x=OF.pc.timeScale().timeToCoordinate(g.time),y=OF.priceSeries.priceToCoordinate(g.price);if(x==null||y==null||x<-50||x>w+50||y<-50||y>h+50)continue;const r=Math.min(OF.C.bubbleMax,3+Math.sqrt(Math.max(1,g.size)/Math.max(1,OF.C.largeQty))*7);const rgb=g.side==='BUY'?'75,211,155':g.side==='SELL'?'255,116,116':'170,170,170';ctx.fillStyle=`rgba(${rgb},${g.proxy?.28:.38})`;ctx.strokeStyle=`rgba(${rgb},${g.proxy?.52:.92})`;ctx.lineWidth=g.size>=OF.C.largeQty?2:1;ctx.beginPath();ctx.arc(x,y,r,0,Math.PI*2);ctx.fill();ctx.stroke();if(g.size>=OF.C.largeQty&&r>=9){ctx.fillStyle='#e8e8e8';ctx.font='9px ui-monospace,monospace';ctx.textAlign='center';ctx.textBaseline='middle';ctx.fillText(String(Math.round(g.size)),x,y)}}};
new ResizeObserver(OF.resizeCanvas).observe($('chartwrap'));
OF.pc.timeScale().subscribeVisibleTimeRangeChange(()=>OF.drawOverlay());
})();
