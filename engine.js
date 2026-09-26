(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.OneSecond=factory();})(typeof globalThis!=='undefined'?globalThis:this,()=>{
  'use strict';
  const LINES=[[0,1,2],[3,4,5],[6,7,8],[0,3,6],[1,4,7],[2,5,8],[0,4,8],[2,4,6]];
  const STEPS=[10,20,25,40,50,100];
  function value(elapsed,step){return Math.floor(Math.max(0,elapsed)/step)*step;}
  function outcome(board){for(const line of LINES)if(board[line[0]]!==null&&line.every(i=>board[i]===board[line[0]]))return{winner:board[line[0]],line};return board.every(x=>x!==null)?{winner:-1,line:[]}:null;}
  class Match{
    constructor({now=0,step=20,online=false}={}){this.step=STEPS.includes(step)?step:20;this.online=online;this.board=Array(9).fill(null);this.phase='countdown';this.until=now+3000;this.starts=[this.until,this.until];this.round=1;this.owner=null;this.result=null;this.candidates=[];this.misses=[0,0];this.lastTap=[-Infinity,-Infinity];this.frozen=[0,0];this.revision=0;}
    snapshot(){return{step:this.step,online:this.online,board:[...this.board],phase:this.phase,until:this.until,starts:[...this.starts],round:this.round,owner:this.owner,result:this.result,candidates:this.candidates.map(x=>({...x})),misses:[...this.misses],frozen:[...this.frozen],revision:this.revision};}
    next(now){this.round++;this.phase='countdown';this.until=now+850;this.starts=[this.until,this.until];this.candidates=[];this.owner=null;this.frozen=[0,0];this.revision++;}
    advance(now){
      if(this.phase==='countdown'&&now>=this.until){this.phase='race';this.revision++;}
      if(this.phase==='deciding'&&now>=this.until){
        const hits=[...this.candidates].sort((a,b)=>a.at-b.at);this.frozen=this.starts.map(t=>value(hits[0].at-t,this.step));
        if(hits.length===2&&Math.abs(hits[0].at-hits[1].at)<=this.step){this.phase='tie';this.until=now+750;this.frozen=[1000,1000];}
        else{this.phase='place';this.owner=hits[0].player;this.frozen[this.owner]=1000;this.until=this.online?now+30000:0;}
        this.revision++;
      }
      if(this.phase==='tie'&&now>=this.until)this.next(now);
      if(this.online&&this.phase==='place'&&now>=this.until)this.finish(1-this.owner,'timeout');
    }
    stop(player,at,receivedAt=at){
      if(![0,1].includes(player)||!['race','deciding'].includes(this.phase)||at<this.starts[player]||at-this.lastTap[player]<90)return false;
      if(this.candidates.some(x=>x.player===player))return false;
      if(this.phase==='deciding'&&at>Math.min(...this.candidates.map(x=>x.at))+this.step)return false;
      this.lastTap[player]=at;
      if(value(at-this.starts[player],this.step)===1000){
        this.candidates.push({player,at});
        if(this.phase==='race'){this.phase='deciding';this.until=receivedAt+(this.online?180:this.step+8);}
      }else{this.starts[player]=at;this.misses[player]++;}
      this.revision++;return true;
    }
    place(player,cell,now){
      if(this.phase!=='place'||player!==this.owner||!Number.isInteger(cell)||cell<0||cell>8||this.board[cell]!==null)return false;
      this.board[cell]=player;const result=outcome(this.board);this.revision++;
      if(result){this.result={...result,reason:'board'};this.phase='ended';}else this.next(now);return true;
    }
    finish(winner,reason='left'){if(this.phase==='ended')return;this.result={winner,line:[],reason};this.phase='ended';this.revision++;}
  }
  function randomNormal(rng=Math.random){return Math.sqrt(-2*Math.log(Math.max(1e-9,rng())))*Math.cos(2*Math.PI*rng());}
  function botDelay(difficulty='normal',step=20,rng=Math.random){
    const chance={easy:.16,normal:.36,hard:.62}[difficulty]??.36;
    if(rng()<chance)return 1000+step*(.12+.76*rng());
    let d=1000+randomNormal(rng)*({easy:140,normal:85,hard:48}[difficulty]||85);
    if(value(d,step)===1000)d=rng()<.5?1000-step*(.4+rng()*2):1000+step*(1.4+rng()*2);
    return Math.max(530,Math.min(1600,d));
  }
  function chooseCell(board,player,difficulty='normal',rng=Math.random){
    const free=board.map((v,i)=>v===null?i:-1).filter(i=>i>=0);if(!free.length)return -1;
    const random=()=>free[Math.floor(rng()*free.length)];
    if(difficulty==='easy'&&rng()<.65)return random();
    for(const p of [player,1-player])for(const i of free){const copy=[...board];copy[i]=p;if(outcome(copy)?.winner===p)return i;}
    if(board[4]===null&&(difficulty==='hard'||rng()<.7))return 4;
    const corners=free.filter(i=>[0,2,6,8].includes(i));return corners.length&&rng()<.7?corners[Math.floor(rng()*corners.length)]:random();
  }
  return{Match,value,outcome,botDelay,chooseCell,STEPS};
});
