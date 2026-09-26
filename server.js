'use strict';
const http=require('node:http');
const crypto=require('node:crypto');
const {promisify}=require('node:util');
const {WebSocketServer,WebSocket}=require('ws');
const {openDatabase}=require('./db');
const {Match,botDelay,chooseCell,STEPS}=require('./engine');
const scrypt=promisify(crypto.scrypt);
const now=()=>performance.timeOrigin+performance.now();
async function passwordHash(password){const salt=crypto.randomBytes(16).toString('hex');const hash=await scrypt(password,salt,64,{N:16384,r:8,p:1});return `${salt}:${hash.toString('hex')}`;}
async function passwordOK(password,encoded){const [salt,hash]=encoded.split(':');const actual=await scrypt(password,salt,64,{N:16384,r:8,p:1});return crypto.timingSafeEqual(actual,Buffer.from(hash,'hex'));}
function fail(message,status=400){throw Object.assign(Error(message),{status});}
async function main(){
  const db=await openDatabase();const sockets=new Set(),byUser=new Map(),rooms=new Map(),queue=[];const limits=new Map(),accountLocks=new Set();let closing=false;
  const origins=new Set((process.env.ALLOWED_ORIGINS||'http://localhost:8080,http://127.0.0.1:8080').split(',').map(x=>x.trim()).filter(Boolean));
  if((process.env.RENDER||process.env.NODE_ENV==='production')&&!process.env.ALLOWED_ORIGINS)throw Error('Set ALLOWED_ORIGINS to your GitHub Pages/custom domain origin.');
  function limited(key,max,ms){const t=Date.now();let entry=limits.get(key);if(!entry||entry.until<=t){entry={n:0,until:t+ms};limits.set(key,entry);}return ++entry.n>max;}
  function ip(req){return process.env.RENDER?String(req.headers['x-forwarded-for']||req.socket.remoteAddress).split(',').pop().trim():req.socket.remoteAddress;}
  const send=(s,data)=>{if(s?.ws.readyState===WebSocket.OPEN)s.ws.send(JSON.stringify(data));};
  const unqueue=s=>{const i=queue.indexOf(s);if(i>=0)queue.splice(i,1);s.queued=false;};
  const busy=u=>{const id=Number(u.id);return queue.some(s=>Number(s.user?.id)===id)||[...rooms.values()].some(r=>r.people.some(s=>Number(s?.user?.id)===id));};
  async function body(req){let data='';for await(const chunk of req){data+=chunk;if(data.length>4096)fail('Request too large',413);}try{return JSON.parse(data||'{}');}catch{fail('Invalid request');}}
  async function auth(req){const token=String(req.headers.authorization||'').replace(/^Bearer /,'');const user=await db.auth(token);if(!user)fail('Please sign in again',401);return{user,token};}
  const server=http.createServer(async(req,res)=>{
    const origin=req.headers.origin;
    res.setHeader('Content-Type','application/json');res.setHeader('Cache-Control','no-store');res.setHeader('X-Content-Type-Options','nosniff');
    if(origin&&origins.has(origin)){res.setHeader('Access-Control-Allow-Origin',origin);res.setHeader('Vary','Origin');}
    res.setHeader('Access-Control-Allow-Headers','Content-Type,Authorization');res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
    const reply=(code,obj)=>{res.writeHead(code);res.end(JSON.stringify(obj));};
    try{
      if(origin&&!origins.has(origin))fail('This website origin is not enabled on the server',403);
      if(req.method==='OPTIONS'){res.writeHead(204);return res.end();}
      const path=new URL(req.url,'http://server').pathname;
      if(path==='/health'&&req.method==='GET')return reply(200,{ok:true,game:'one-second'});
      if(limited(`http:${ip(req)}`,180,60000))fail('Too many requests. Try again shortly.',429);
      if(path==='/api/leaderboard'&&req.method==='GET')return reply(200,{rows:await db.leaderboard(new URL(req.url,'http://server').searchParams.get('sort'))});
      if((path==='/api/register'||path==='/api/login')&&req.method==='POST'){
        if(limited(`auth:${ip(req)}`,15,600000))fail('Too many attempts. Try again in 10 minutes.',429);
        const b=await body(req);if(typeof b.username!=='string'||!/^[A-Za-z0-9]{3,8}$/.test(b.username))fail('Username: 3–8 letters or numbers');
        if(typeof b.password!=='string'||!/^[\x21-\x7E]{8,128}$/.test(b.password))fail('Password: 8–128 letters, numbers or symbols; no spaces');
        let u=await db.find(b.username);
        if(path==='/api/register'){
          if(u)fail('That username is taken',409);
          try{u=await db.create(b.username,await passwordHash(b.password));}catch(e){if(e.code==='23505'||String(e.message).includes('UNIQUE constraint'))fail('That username is taken',409);throw e;}
        }else{const encoded=u?.password||await dummyHash;if(!await passwordOK(b.password,encoded)||!u)fail('Incorrect username or password',401);}
        return reply(200,{user:db.publicUser(u),token:await db.session(u),stats:await db.stats(u.id)});
      }
      if(path==='/api/me'&&req.method==='GET'){const {user}=await auth(req);return reply(200,{user:db.publicUser(user),stats:await db.stats(user.id)});}
      if(path==='/api/logout'&&req.method==='POST'){const {user,token}=await auth(req);await db.logout(token);byUser.get(Number(user.id))?.ws.close(1000,'Signed out');return reply(200,{ok:true});}
      if(['/api/reset','/api/delete'].includes(path)&&req.method==='POST'){
        const {user}=await auth(req);const b=await body(req);
        if(limited(`sensitive:${user.id}`,8,600000))fail('Try again later',429);
        if(busy(user)||accountLocks.has(Number(user.id)))fail('Finish or leave your match first',409);
        accountLocks.add(Number(user.id));
        try{
          if(typeof b.password!=='string'||b.password.length>128||!await passwordOK(b.password,user.password))fail('Incorrect password',401);
          if(path==='/api/reset')await db.reset(user.id);else{await db.remove(user);byUser.get(Number(user.id))?.ws.close(1000,'Account deleted');}
          return reply(200,{ok:true});
        }finally{accountLocks.delete(Number(user.id));}
      }
      reply(404,{error:'Not found'});
    }catch(e){if(!e.status)console.error('HTTP operation failed:',e.code||e.name);reply(e.status||503,{error:e.status?e.message:'Server unavailable. Please try again.'});}
  });
  const dummyHash=passwordHash('not-a-user-password');
  const wss=new WebSocketServer({noServer:true,maxPayload:4096,perMessageDeflate:false});
  server.on('upgrade',(req,socket,head)=>{
    if(closing||req.url!=='/ws'||!origins.has(req.headers.origin)||limited(`ws:${ip(req)}`,40,60000)||sockets.size>=1000){socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');return socket.destroy();}
    wss.handleUpgrade(req,socket,head,ws=>wss.emit('connection',ws,req));
  });
  function broadcast(room){const state=room.game.snapshot();for(let i=0;i<room.people.length;i++)send(room.people[i],{type:'state',state,serverTime:now(),you:i,mode:room.mode,names:room.names,matchId:room.id});}
  function start(people,mode,difficulty='normal',step=20){
    const room={id:crypto.randomUUID(),people,mode,difficulty,game:new Match({now:now()+200,step,online:mode==='online'}),recording:false,retryAt:0,botAt:0,botStart:null,botMoveAt:0};
    room.names=people.map((p,i)=>p?.user?.username||(mode==='bot'&&i===1?'Bot':'Guest'));rooms.set(room.id,room);
    for(const s of people)if(s){unqueue(s);s.room=room;s.strikes=0;}broadcast(room);
  }
  function matchmake(){while(queue.length>=2){const a=queue.shift(),b=queue.shift();if(a.ws.readyState!==1||b.ws.readyState!==1){for(const s of[a,b])if(s.ws.readyState===1)queue.unshift(s);continue;}start(Math.random()<.5?[a,b]:[b,a],'online');}}
  async function finish(room){
    if(room.recording||now()<room.retryAt)return;room.recording=true;
    try{await db.record(room);for(const s of room.people)if(s){if(s.room===room)s.room=null;send(s,{type:'saved',matchId:room.id});}rooms.delete(room.id);}
    catch(e){console.error('Result persistence failed:',e.code||e.name);room.recording=false;room.retryAt=now()+5000;for(const s of room.people)send(s,{type:'notice',message:'Saving result…'});}
  }
  function leave(s){unqueue(s);if(s.room){const room=s.room;if(room.game.phase!=='ended'){room.game.finish(1-room.people.indexOf(s),'left');broadcast(room);}void finish(room);}}
  wss.on('connection',(ws,req)=>{
    const s={ws,user:null,ready:false,room:null,queued:false,ip:ip(req),rtt:0,pingAt:0,pingNonce:'',lastPong:now(),messages:0,window:now(),chain:Promise.resolve()};sockets.add(s);
    const authTimeout=setTimeout(()=>{if(!s.ready)ws.close(1008,'Sign in first');},10000);
    ws.on('pong',()=>{s.alive=true;});s.alive=true;
    ws.on('message',raw=>{s.chain=s.chain.then(async()=>{
      if(now()-s.window>1000){s.window=now();s.messages=0;}if(++s.messages>35)return ws.close(1008,'Slow down');
      let m;try{m=JSON.parse(raw.toString());}catch{return ws.close(1008,'Invalid message');}if(!m||typeof m!=='object')return;
      if(m.type==='hello'){
        if(s.ready)return;if(m.token){s.user=await db.auth(m.token);if(ws.readyState!==WebSocket.OPEN)return;if(!s.user)return ws.close(4001,'Please sign in again');if(byUser.has(Number(s.user.id)))return ws.close(4002,'Account already open in another tab');byUser.set(Number(s.user.id),s);}
        s.ready=true;clearTimeout(authTimeout);send(s,{type:'ready',serverTime:now()});return;
      }
      if(!s.ready)return;
      if(m.type==='sync'){if(typeof m.at==='number'&&Number.isFinite(m.at))send(s,{type:'sync',at:m.at,serverTime:now()});return;}
      if(m.type==='pong'&&m.nonce===s.pingNonce){const sample=now()-s.pingAt;s.rtt=s.rtt?Math.min(s.rtt,sample):sample;s.pingNonce='';s.lastPong=now();return;}
      if(m.type==='queue'){
        if(!s.user)return send(s,{type:'error',message:'Sign in to play online'});if(s.room||s.queued)return;
        if(accountLocks.has(Number(s.user.id))||busy(s.user))return send(s,{type:'error',message:'Your previous match is still saving. Try again shortly.'});
        s.queued=true;queue.push(s);send(s,{type:'queued'});matchmake();return;
      }
      if(m.type==='cancel'){unqueue(s);send(s,{type:'cancelled'});return;}
      if(m.type==='bot'){
        if(s.room||s.queued)return;if(limited(`bot:${s.ip}`,20,60000))return send(s,{type:'error',message:'Take a moment before starting another match'});
        if(s.user&&(accountLocks.has(Number(s.user.id))||busy(s.user)))return send(s,{type:'error',message:'Your previous match is still saving. Try again shortly.'});
        start([s,null],'bot',['easy','normal','hard'].includes(m.difficulty)?m.difficulty:'normal',STEPS.includes(m.step)?m.step:20);return;
      }
      if(m.type==='leave'){leave(s);return;}
      if(!s.room||m.matchId!==s.room.id)return;const room=s.room,game=room.game,player=room.people.indexOf(s);const t=now();game.advance(t);
      if(m.type==='stop'&&m.round===game.round){
        // Timing is judged by the server. No client-supplied elapsed time or win claims.
        const corrected=Math.max(game.starts[player],t-Math.min(100,s.rtt/2));game.stop(player,corrected,t);broadcast(room);
      }
      if(m.type==='place'&&game.place(player,m.cell,t))broadcast(room);
      if(game.phase==='ended')void finish(room);
    }).catch(e=>{console.error('Socket operation failed:',e.code||e.name);send(s,{type:'error',message:'Server unavailable. Please try again.'});});});
    ws.on('close',()=>{clearTimeout(authTimeout);sockets.delete(s);if(s.user&&byUser.get(Number(s.user.id))===s)byUser.delete(Number(s.user.id));if(!closing)leave(s);});
    ws.on('error',()=>{});
  });
  const tick=setInterval(()=>{
    const t=now();for(const room of rooms.values()){
      const g=room.game,before=g.revision;g.advance(t);
      if(room.mode==='bot'){
        if(['race','deciding'].includes(g.phase)){
          if(room.botStart!==g.starts[1]){room.botStart=g.starts[1];room.botAt=room.botStart+botDelay(room.difficulty,g.step);}
          if(t>=room.botAt){g.stop(1,room.botAt,t);room.botStart=null;}
        }
        if(g.phase==='place'&&g.owner===1){if(!room.botMoveAt)room.botMoveAt=t+330+Math.random()*400;if(t>=room.botMoveAt){g.place(1,chooseCell(g.board,1,room.difficulty),t);room.botMoveAt=0;}}
      }
      if(g.revision!==before)broadcast(room);if(g.phase==='ended')void finish(room);
    }
  },5);
  const heartbeat=setInterval(()=>{for(const s of sockets){if(!s.alive){s.ws.terminate();continue;}s.alive=false;s.ws.ping();if(!s.pingNonce||now()-s.pingAt>10000){s.pingNonce=crypto.randomBytes(8).toString('hex');s.pingAt=now();send(s,{type:'ping',nonce:s.pingNonce});}}},3000);
  let dispatching=false;
  const hookURLs={account:process.env.ACCOUNT_WEBHOOK_URL,multiplayer:process.env.MULTIPLAYER_WEBHOOK_URL,bot:process.env.BOT_WEBHOOK_URL};
  for(const [name,url]of Object.entries(hookURLs))if(url&&!/^https:\/\/discord\.com\/api\/webhooks\/\d+\/[\w-]+$/.test(url))throw Error(`Invalid ${name} webhook configuration`);
  const dispatcher=setInterval(async()=>{
    if(dispatching)return;dispatching=true;try{
      const rows=await db.q('SELECT * FROM outbox WHERE due<=$1 ORDER BY id LIMIT 5',[Date.now()]);
      for(const row of rows){const url=hookURLs[row.channel];if(!url){await db.q('DELETE FROM outbox WHERE id=$1',[row.id]);continue;}
        let delivered=false,delay=Math.min(3600000,2000*2**Math.min(row.attempts,10));
        try{const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:row.message,allowed_mentions:{parse:[]}}),signal:AbortSignal.timeout(8000)});delivered=r.ok;if(r.status===429){const j=await r.json().catch(()=>({}));delay=Math.max(delay,Number(j.retry_after||5)*1000);}if(r.status===401||r.status===404)delay=3600000;}catch{}
        if(delivered)await db.q('DELETE FROM outbox WHERE id=$1',[row.id]);else await db.q('UPDATE outbox SET attempts=attempts+1,due=$1 WHERE id=$2',[Date.now()+delay,row.id]);
      }
    }catch(e){console.error('Webhook delivery deferred:',e.code||e.name);}finally{dispatching=false;}
  },1500);
  const cleanup=setInterval(()=>{for(const[k,v]of limits)if(v.until<Date.now())limits.delete(k);void db.clean().catch(()=>{});},60000);
  server.listen(Number(process.env.PORT)||3000,'0.0.0.0',()=>console.log(`ONE SECOND listening on ${server.address().port}`));
  async function shutdown(){if(closing)return;closing=true;clearInterval(tick);clearInterval(heartbeat);clearInterval(dispatcher);clearInterval(cleanup);for(const s of sockets){send(s,{type:'shutdown'});s.ws.close(1012,'Server restarting');}server.close();setTimeout(()=>process.exit(0),1000).unref();}
  process.on('SIGTERM',shutdown);process.on('SIGINT',shutdown);
  return{server,db,wss};
}
if(require.main===module)main().catch(e=>{console.error('Startup failed:',e.message);process.exit(1);});
module.exports={main,passwordHash,passwordOK};
