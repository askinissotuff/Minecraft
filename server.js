import http from 'node:http';
import { randomBytes, randomUUID } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();
const MAX_ROOMS = 200;
const MAX_PLAYERS = 8;
const MAX_CHAT = 240;
const clean = (v, max=20) => String(v ?? '').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0,max) || 'Player';
const code = () => randomBytes(3).toString('hex').toUpperCase();
const server = http.createServer((req,res) => {
  const url = new URL(req.url, 'http://localhost');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Content-Type','application/json; charset=utf-8');
  if(req.method==='OPTIONS'){res.writeHead(204);return res.end();}
  if(url.pathname==='/health'){res.writeHead(200);return res.end(JSON.stringify({ok:true,service:'voxel-realms',rooms:rooms.size}));}
  if(url.pathname==='/rooms'){res.writeHead(200);return res.end(JSON.stringify([...rooms.values()].map(r=>({code:r.code,name:r.name,players:r.players.size,maxPlayers:MAX_PLAYERS})).filter(r=>r.players>0)));}
  res.writeHead(200);res.end(JSON.stringify({name:'Voxel Realms Network',status:'online',websocket:'/ws'}));
});
const wss = new WebSocketServer({server,path:'/ws',maxPayload:4096});
function send(ws,data){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(data));}
function broadcast(room,data,except=null){for(const p of room.players.values())if(p.ws!==except)send(p.ws,data);}
function snapshot(room){return [...room.players.values()].map(p=>({id:p.id,name:p.name,x:p.x,y:p.y,z:p.z,yaw:p.yaw,pitch:p.pitch}));}
function remove(ws){const room=ws.room;if(!room)return;room.players.delete(ws.playerId);broadcast(room,{type:'leave',id:ws.playerId});if(!room.players.size)rooms.delete(room.code);ws.room=null;ws.playerId=null;}
wss.on('connection',ws=>{
  ws.isAlive=true;ws.on('pong',()=>ws.isAlive=true);
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw.toString())}catch{return}
    if(!m||typeof m.type!=='string')return;
    if(m.type==='create'){
      if(ws.room)remove(ws);
      if(rooms.size>=MAX_ROOMS){send(ws,{type:'error',message:'Lobby capacity reached. Try again later.'});return}
      let id;do{id=code()}while(rooms.has(id));
      const room={code:id,name:clean(m.name,28),players:new Map(),world:new Map(),created:Date.now()};
      rooms.set(id,room);join(ws,room,m.name);return;
    }
    if(m.type==='join'){
      const room=rooms.get(clean(m.code,8).toUpperCase());
      if(!room){send(ws,{type:'error',message:'That lobby code was not found.'});return}
      if(room.players.size>=MAX_PLAYERS){send(ws,{type:'error',message:'This lobby is full.'});return}
      if(ws.room)remove(ws);join(ws,room,m.name);return;
    }
    if(m.type==='list'){send(ws,{type:'rooms',rooms:[...rooms.values()].map(r=>({code:r.code,name:r.name,players:r.players.size,maxPlayers:MAX_PLAYERS}))});return}
    if(!ws.room)return;
    const room=ws.room,player=room.players.get(ws.playerId);if(!player)return;
    if(m.type==='block'){
      const x=Math.round(Number(m.x)),y=Math.round(Number(m.y)),z=Math.round(Number(m.z));
      const allowed=new Set(['grass','dirt','stone','sand','wood','leaves','water','bedrock','planks','coal','iron','farmland','crop','wheat']);
      if(![x,y,z].every(Number.isFinite)||Math.abs(x)>10000||Math.abs(y)>256||Math.abs(z)>10000)return;
      const block=allowed.has(m.block)?m.block:null;
      const id=x+','+y+','+z;if(block){if(room.world.size<30000||room.world.has(id))room.world.set(id,block)}else room.world.delete(id);broadcast(room,{type:'block',x,y,z,block,by:player.id});return;
    }
    if(m.type==='move'){
      const n=v=>Number.isFinite(Number(v))?Math.max(-10000,Math.min(10000,Number(v))):0;
      player.x=n(m.x);player.y=n(m.y);player.z=n(m.z);player.yaw=n(m.yaw);player.pitch=Math.max(-1.5,Math.min(1.5,n(m.pitch)));
      const now=Date.now();if(now-(ws.lastMove||0)<45)return;ws.lastMove=now;
      broadcast(room,{type:'move',player:{id:player.id,name:player.name,x:player.x,y:player.y,z:player.z,yaw:player.yaw,pitch:player.pitch}},ws);return;
    }
    if(m.type==='chat'){
      const text=clean(m.text,MAX_CHAT);if(!text)return;
      broadcast(room,{type:'chat',id:player.id,name:player.name,text,at:Date.now()});return;
    }
    if(m.type==='leave'){remove(ws);send(ws,{type:'left'});}
  });
  ws.on('close',()=>remove(ws));ws.on('error',()=>remove(ws));
});
function join(ws,room,name){
  const id=randomUUID();const p={id,name:clean(name),ws,x:0,y:4,z:0,yaw:0,pitch:0};
  room.players.set(id,p);ws.room=room;ws.playerId=id;
  send(ws,{type:'joined',code:room.code,roomName:room.name,self:id,players:snapshot(room),blocks:[...room.world],maxPlayers:MAX_PLAYERS});
  broadcast(room,{type:'enter',player:{id,name:p.name,x:0,y:4,z:0,yaw:0,pitch:0}},ws);
}
const heartbeat=setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue}ws.isAlive=false;ws.ping()}},25000);
server.listen(PORT,'0.0.0.0',()=>console.log('Voxel Realms listening on '+PORT));
process.on('SIGTERM',()=>{clearInterval(heartbeat);for(const ws of wss.clients)ws.close();server.close();});
