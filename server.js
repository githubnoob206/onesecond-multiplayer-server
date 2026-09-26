'use strict';
const http=require('node:http');
const {randomUUID,randomBytes}=require('node:crypto');
const {WebSocketServer,WebSocket}=require('ws');
const STEPS=[10,20,25,40,50,100];
function createApp(){
 const clients=new Set(),lobbies=new Map(),limits=new Map(),seen=new Map(),outbox=[];
 const origins=new Set((process.env.ALLOWED_ORIGINS||'http://localhost:8080,http://127.0.0.1:8080').split(',').map(s=>s.trim()).filter(Boolean));
 const hooks={lobby:process.env.LOBBY_WEBHOOK_URL||process.env.ACCOUNT_WEBHOOK_URL,peer:process.env.MULTIPLAYER_WEBHOOK_URL,bot:process.env.BOT_WEBHOOK_URL};
 for(const[k,v]of Object.entries(hooks))if(v&&!/^https:\/\/discord\.com\/api\/webhooks\/\d+\/[\w-]+$/.test(v))throw Error('Invalid '+k+' webhook URL');
 let closing=false,dispatching=false;
 const send=(c,m)=>{if(c?.ws.readyState===WebSocket.OPEN)c.ws.send(JSON.stringify(m));};
 const address=req=>process.env.RENDER?String(req.headers['x-forwarded-for']||req.socket.remoteAddress).split(',').pop().trim():req.socket.remoteAddress;
 function rate(key,max,ms){const t=Date.now();let e=limits.get(key);if(!e||e.until<t){e={count:0,until:t+ms};limits.set(key,e);}return ++e.count>max;}
 function log(channel,message,id=randomUUID()){
  if(seen.has(id))return true;if(!hooks[channel])return true;if(outbox.length>=1000)return false;
  seen.set(id,Date.now());outbox.push({channel,message,at:0,tries:0});return true;
 }
 function publicRoom(r){return{id:r.id,host:r.host.name,guest:r.guest?.name||null,settings:r.settings,playing:r.playing,ready:!!(r.host.ready&&r.guest?.ready)};}
 function list(c){send(c,{type:'list',rooms:[...lobbies.values()].filter(r=>!r.playing&&!r.guest).map(publicRoom)});}
 function lists(){for(const c of clients)if(c.hello&&!c.room)list(c);}
 function roomState(r){send(r.host,{type:'room',room:publicRoom(r),host:true});send(r.guest,{type:'room',room:publicRoom(r),host:false});}
 const side=(r,c)=>c===r.host?r.settings.hostMark:1-r.settings.hostMark;
 function result(r,winner,reason){
  if(!r.playing)return;r.playing=false;
  const names=[];names[r.settings.hostMark]=r.host.name;names[1-r.settings.hostMark]=r.guest?.name||'Guest';
  const label=winner===-1?`Draw • ${names.join(' vs ')}`:`Win • ${names[winner]}\nLoss • ${names[1-winner]}`;
  log('peer',`${label}\nPeer match • ${reason} • ${r.matchId}\nHost-reported, unverified result.`,r.matchId);
  const m={type:'finished',matchId:r.matchId,winner,reason};send(r.host,m);send(r.guest,m);roomState(r);lists();
 }
 function leave(c){const r=c.room;if(!r)return;
  if(r.playing)result(r,1-side(r,c),'left');
  c.room=null;c.ready=false;
  if(c===r.host){lobbies.delete(r.id);if(r.guest){r.guest.room=null;r.guest.ready=false;send(r.guest,{type:'closed',message:'Host left'});}log('lobby',`Lobby closed • ${r.id} • ${r.host.name}`);}
  else{r.guest=null;r.host.ready=false;send(r.host,{type:'peer-left'});roomState(r);}
  send(c,{type:'left'});lists();
 }
 const server=http.createServer(async(req,res)=>{
  const origin=req.headers.origin;res.setHeader('Content-Type','application/json');res.setHeader('Cache-Control','no-store');res.setHeader('X-Content-Type-Options','nosniff');
  if(origin&&origins.has(origin)){res.setHeader('Access-Control-Allow-Origin',origin);res.setHeader('Vary','Origin');}
  res.setHeader('Access-Control-Allow-Headers','Content-Type');res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  const reply=(status,data)=>{res.writeHead(status);res.end(JSON.stringify(data));};
  try{
   if(origin&&!origins.has(origin))return reply(403,{error:'Website origin not allowed'});
   if(req.method==='OPTIONS'){res.writeHead(204);return res.end();}
   if(req.url==='/health')return reply(200,{ok:true,game:'one-second-p2p',database:false});
   if(req.method==='POST'&&req.url==='/api/report'){
    if(rate('report:'+address(req),50,600000))return reply(429,{error:'Try later'});
    let text='';for await(const part of req){text+=part;if(text.length>2048)return reply(413,{error:'Too large'});}const m=JSON.parse(text);
    if(!m||typeof m.id!=='string'||!/^[a-f0-9-]{36}$/.test(m.id)||typeof m.name!=='string'||!/^[A-Za-z0-9]{3,12}$/.test(m.name)||!['easy','normal','hard'].includes(m.difficulty)||![-1,0,1].includes(m.winner)||!['board','left','timeout'].includes(m.reason))return reply(400,{error:'Invalid report'});
    const label=m.winner===-1?`Draw • ${m.name} vs Bot`:m.winner===0?`Win • ${m.name}\nLoss • Bot`:`Win • Bot\nLoss • ${m.name}`;
    const queued=log('bot',`${label}\nBot (${m.difficulty}) • ${m.reason} • ${m.id}\nDevice-reported, unverified result.`,m.id);
    return reply(queued?200:503,{ok:queued});
   }
   return reply(404,{error:'Not found'});
  }catch{return reply(400,{error:'Invalid request'});}
 });
 const wss=new WebSocketServer({noServer:true,maxPayload:16384,perMessageDeflate:false});
 server.on('upgrade',(req,socket,head)=>{
  if(closing||req.url!=='/ws'||!origins.has(req.headers.origin)||clients.size>=500||rate('ws:'+address(req),40,60000)){socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');return socket.destroy();}
  wss.handleUpgrade(req,socket,head,ws=>wss.emit('connection',ws,req));
 });
 wss.on('connection',(ws,req)=>{
  const c={ws,ip:address(req),name:'Guest',hello:false,room:null,ready:false,alive:true};clients.add(c);
  const timeout=setTimeout(()=>{if(!c.hello)ws.close(1008,'Say hello');},10000);
  ws.on('pong',()=>c.alive=true);
  ws.on('message',raw=>{try{
   if(rate('msg:'+c.ip,1200,10000))return ws.close(1008,'Too many messages');
   const m=JSON.parse(raw);if(!m||typeof m!=='object')return;
   if(m.type==='hello'&&!c.hello){if(typeof m.name!=='string'||!/^[A-Za-z0-9]{3,12}$/.test(m.name))return send(c,{type:'error',message:'Name: 3–12 letters or numbers'});c.name=m.name;c.hello=true;clearTimeout(timeout);send(c,{type:'ready'});list(c);return;}
   if(!c.hello)return;
   if(m.type==='list'){list(c);return;}
   if(m.type==='leave'){leave(c);return;}
   if(m.type==='create'){
    if(c.room)return;if(rate('create:'+c.ip,20,600000))return send(c,{type:'error',message:'Too many lobbies. Try again later.'});
    let id;do{id=randomBytes(3).toString('hex').toUpperCase();}while(lobbies.has(id));
    const r={id,host:c,guest:null,settings:{step:20,hostMark:0,moveSeconds:30},playing:false,matchId:null};lobbies.set(id,r);c.room=r;c.ready=false;
    log('lobby',`Lobby created • ${id} • ${c.name}`);roomState(r);lists();return;
   }
   if(m.type==='join'){
    if(c.room)return;const r=lobbies.get(m.id);if(!r||r.guest||r.playing)return send(c,{type:'error',message:'That lobby is no longer open'});
    r.guest=c;c.room=r;c.ready=false;r.host.ready=false;roomState(r);lists();return;
   }
   const r=c.room;if(!r)return;const other=c===r.host?r.guest:r.host;
   if(m.type==='settings'&&c===r.host&&!r.playing){if(STEPS.includes(m.step)&&[0,1].includes(m.hostMark)&&[15,30,60].includes(m.moveSeconds)){r.settings={step:m.step,hostMark:m.hostMark,moveSeconds:m.moveSeconds};roomState(r);}return;}
   if(m.type==='link-ready'){c.ready=!!m.ready;roomState(r);return;}
   if(m.type==='signal'||m.type==='relay'){
    if(!other||m.roomId!==r.id||!m.data||typeof m.data!=='object')return;
    send(other,{type:m.type,data:m.data,roomId:r.id});return;
   }
   if(m.type==='start'&&c===r.host&&!r.playing){
    if(!r.guest||!r.host.ready||!r.guest.ready)return send(c,{type:'error',message:'Wait for the other player to connect'});
    r.playing=true;r.matchId=randomUUID();const msg={type:'start',matchId:r.matchId,room:publicRoom(r)};send(r.host,msg);send(r.guest,msg);lists();return;
   }
   if(m.type==='result'&&c===r.host&&r.playing&&m.matchId===r.matchId&&[-1,0,1].includes(m.winner)&&['board','left','timeout'].includes(m.reason))result(r,m.winner,m.reason);
  }catch{send(c,{type:'error',message:'Invalid message'});}});
  ws.on('close',()=>{clearTimeout(timeout);clients.delete(c);if(!closing)leave(c);});ws.on('error',()=>{});
 });
 const heartbeat=setInterval(()=>{for(const c of clients){if(!c.alive){c.ws.terminate();continue;}c.alive=false;c.ws.ping();}},15000);
 const clean=setInterval(()=>{for(const[k,v]of limits)if(v.until<Date.now())limits.delete(k);for(const[k,t]of seen)if(t<Date.now()-86400000)seen.delete(k);},60000);
 const dispatcher=setInterval(async()=>{
  if(dispatching)return;dispatching=true;try{
   const e=outbox.find(e=>e.at<=Date.now());if(!e)return;
   let ok=false,delay=Math.min(60000,2000*2**e.tries);
   try{const r=await fetch(hooks[e.channel],{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:e.message,allowed_mentions:{parse:[]}}),signal:AbortSignal.timeout(5000)});ok=r.ok;if(r.status===429){const j=await r.json().catch(()=>({}));delay=Math.max(delay,Number(j.retry_after||5)*1000);}}catch{}
   e.tries++;e.at=Date.now()+delay;if(ok||e.tries>=8)outbox.splice(outbox.indexOf(e),1);
  }finally{dispatching=false;}
 },1000);
 function shutdown(){if(closing)return;closing=true;clearInterval(heartbeat);clearInterval(clean);clearInterval(dispatcher);for(const c of clients){send(c,{type:'shutdown'});c.ws.close(1012,'Restarting');}server.close();}
 return{server,wss,shutdown,lobbies};
}
if(require.main===module){try{const app=createApp();app.server.listen(Number(process.env.PORT)||3000,'0.0.0.0',()=>console.log('ONE SECOND P2P listening on '+app.server.address().port));for(const signal of ['SIGTERM','SIGINT'])process.on(signal,()=>{app.shutdown();setTimeout(()=>process.exit(0),1000).unref();});}catch(e){console.error(e.message);process.exit(1);}}
module.exports={createApp};
