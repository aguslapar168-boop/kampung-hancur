// Kampung Hancur - Klee & Lumine | 1 file: server + game + tunnel Cloudflare
// Termux: pkg install nodejs cloudflared -y  ->  node server.js  ->  buka link trycloudflare yang muncul
const http=require('http'),fs=require('fs'),crypto=require('crypto'),{spawn}=require('child_process');
const PORT=+process.env.PORT||3000;
const SRC=fs.readFileSync(__filename,'utf8'),A='/*<<'+'HTML',B='HTML'+'>>*/';
const HTML=SRC.slice(SRC.indexOf(A)+A.length,SRC.lastIndexOf(B));
const ICON='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><rect width="512" height="512" fill="#0b0f1a"/><circle cx="256" cy="256" r="190" fill="#ff7a2f"/><text x="256" y="320" font-size="220" text-anchor="middle">🧟</text></svg>';
const MANIFEST=JSON.stringify({name:'Kampung Hancur',short_name:'Kampung',start_url:'/',display:'fullscreen',orientation:'landscape',background_color:'#0b0f1a',theme_color:'#0b0f1a',icons:[{src:'/icon.svg',sizes:'any',type:'image/svg+xml',purpose:'any maskable'}]});
const srv=http.createServer((q,r)=>{if(q.url=='/manifest.json'){r.writeHead(200,{'Content-Type':'application/manifest+json'});return r.end(MANIFEST)}if(q.url=='/icon.svg'){r.writeHead(200,{'Content-Type':'image/svg+xml'});return r.end(ICON)}r.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});r.end(HTML)});
// ---- WebSocket minimal (tanpa npm) ----
const rooms=new Map();let NID=1;
function frame(s){const b=Buffer.from(s),l=b.length;let h;if(l<126)h=Buffer.from([0x81,l]);else if(l<65536){h=Buffer.alloc(4);h[0]=0x81;h[1]=126;h.writeUInt16BE(l,2)}else{h=Buffer.alloc(10);h[0]=0x81;h[1]=127;h.writeBigUInt64BE(BigInt(l),2)}return Buffer.concat([h,b])}
const send=(c,s)=>{if(!c.sock.destroyed)c.sock.write(frame(s))};
const bc=(r,from,s)=>r.m.forEach(x=>{if(x!==from)send(x,s)});
function onData(c,d){c.buf=Buffer.concat([c.buf,d]);for(;;){const b=c.buf;if(b.length<2)return;let len=b[1]&127,o=2;if(len==126){if(b.length<4)return;len=b.readUInt16BE(2);o=4}else if(len==127){if(b.length<10)return;len=Number(b.readBigUInt64BE(2));o=10}
 const mk=b[1]&128;if(mk)o+=4;if(len>1e6){c.sock.destroy();return}if(b.length<o+len)return;const p=Buffer.from(b.slice(o,o+len));if(mk){const k=b.slice(o-4,o);for(let i=0;i<len;i++)p[i]^=k[i&3]}c.buf=b.slice(o+len);const op=b[0]&15;
 if(op==8){c.sock.end();return}if(op==9)c.sock.write(Buffer.concat([Buffer.from([0x8a,len]),p]));else if(op==1)onMsg(c,p.toString())}}
const lbList=r=>r.m.map(x=>({id:x.id,n:x.n,h:x.h||null,d:x.d?1:0,slot:x.slot}));
function go(r){r.started=1;const o=JSON.stringify({t:'go',pl:r.m.filter(x=>x.d).map(x=>({id:x.id,n:x.n,h:x.h,slot:x.slot}))});r.m.forEach(x=>send(x,o))}
function onMsg(c,s){let m;try{m=JSON.parse(s)}catch(e){return}
 if(m.t=='join'){if(c.room)return;const code=String(m.c||'X').toUpperCase().slice(0,8),r=rooms.get(code)||{m:[],host:0,started:0};if(r.m.length>=2){send(c,'{"t":"full"}');return}rooms.set(code,r);
  c.id=NID++;c.n=String(m.n||'Pemain').slice(0,10);c.h=null;c.d=0;c.room=code;c.slot=r.m.some(x=>x.slot==1)?2:1;if(!r.m.length)r.host=c.id;
  send(c,JSON.stringify({t:'w',id:c.id,slot:c.slot,host:r.host==c.id,pl:lbList(r)}));r.m.push(c);bc(r,c,JSON.stringify({t:'j',id:c.id,n:c.n,slot:c.slot}));console.log('['+code+'] '+c.n+' masuk lobi ('+r.m.length+'/2)');return}
 const r=rooms.get(c.room);if(!r)return;
 const lb=()=>{const o=JSON.stringify({t:'lb',id:c.id,h:c.h,d:c.d});r.m.forEach(x=>send(x,o))};
 if(m.t=='pick'){if((m.h=='k'||m.h=='l'||m.h=='a')&&!r.m.some(x=>x!==c&&x.h==m.h)){c.h=m.h;c.d=0}lb();return}
 if(m.t=='done'){c.d=c.h&&m.d?1:0;lb();if(c.d&&(r.started||(r.m.length==2&&r.m.every(x=>x.d))))go(r);return}
 if(m.t=='solo'){if(c.d&&r.m.length==1)go(r);return}
 m.f=c.id;const o=JSON.stringify(m);if(m.to){const x=r.m.find(q=>q.id==m.to);if(x)send(x,o)}else bc(r,c,o)}
function onClose(c){if(c.gone)return;c.gone=1;const r=rooms.get(c.room);if(!r)return;r.m=r.m.filter(x=>x!==c);console.log('['+c.room+'] '+c.n+' keluar');if(!r.m.length){rooms.delete(c.room);return}bc(r,c,JSON.stringify({t:'l',id:c.id}));if(r.host==c.id){r.host=r.m[0].id;r.m.forEach(x=>send(x,JSON.stringify({t:'host',id:r.host})))}}
srv.on('upgrade',(req,sock)=>{const k=req.headers['sec-websocket-key'];if(!k){sock.destroy();return}
 sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+crypto.createHash('sha1').update(k+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64')+'\r\n\r\n');
 sock.setNoDelay(true);const c={sock,buf:Buffer.alloc(0)};sock.on('data',d=>onData(c,d));sock.on('close',()=>onClose(c));sock.on('error',()=>onClose(c))});
setInterval(()=>{for(const r of rooms.values())for(const c of r.m)if(!c.sock.destroyed)c.sock.write(Buffer.from([0x89,0]))},25000);
// ---- Cloudflare tunnel otomatis ----
let cf=null,noCf=0;
function tunnel(){cf=spawn('cloudflared',['tunnel','--no-autoupdate','--url','http://127.0.0.1:'+PORT]);let shown=0;
 const f=d=>{const m=String(d).match(/https:\/\/[a-z0-9-]+\.trycloudflare\.com/);if(m&&!shown){shown=1;console.log('\n==================================================\n  LINK MAIN (kirim ke teman):\n  '+m[0]+'\n  Isi nama + kode tim yang sama buat mabar (maks 2)\n==================================================\n')}};
 cf.stdout.on('data',f);cf.stderr.on('data',f);cf.on('error',()=>{noCf=1;console.log('cloudflared belum terpasang. Jalankan: pkg install cloudflared')});cf.on('exit',()=>{if(!noCf){console.log('tunnel mati, nyambung ulang...');setTimeout(tunnel,3000)}})}
process.on('exit',()=>{if(cf)cf.kill()});process.on('SIGINT',()=>process.exit());
srv.listen(PORT,'127.0.0.1',()=>{console.log('Server jalan, membuat link Cloudflare...');tunnel()});

/*<<HTML
<!DOCTYPE html><html lang="id"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
<link rel="manifest" href="/manifest.json"><meta name="mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="theme-color" content="#0b0f1a"><title>Kampung Hancur: Klee, Lumine &amp; Aether</title>
<style>
:root{color-scheme:dark;--a:#ff7a2f;--bg:#0b0f1a;--tx:#fff;box-sizing:border-box;padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}
html,body{margin:0;height:100%;overflow:hidden;background:var(--bg);color:var(--tx);font:700 14px system-ui,sans-serif;touch-action:none;user-select:none;-webkit-user-select:none}
canvas{display:block}
#hud{position:fixed;inset:0;pointer-events:none;display:none}
#top{position:absolute;top:calc(8px + env(safe-area-inset-top,0px));left:10px;right:110px;text-shadow:0 2px 0 #000}
.bar{width:170px;height:9px;background:#0009;border-radius:6px;overflow:hidden;margin-top:4px}.bar i{display:block;height:100%;background:#5fd35f;width:100%}
#rd{position:absolute;top:calc(8px + env(safe-area-inset-top,0px));right:8px;width:92px;height:92px;border-radius:50%;background:#0007;border:2px solid #fff5}
#ms{position:absolute;top:34%;left:0;right:0;text-align:center;font-size:22px;text-shadow:0 2px 6px #000;opacity:0;transition:opacity .4s}
button{font:inherit;color:#fff;border:0;border-radius:50%;pointer-events:auto;background:#2b3a66;box-shadow:0 4px 0 #141c38;position:absolute}
#b0{right:16px;bottom:calc(26px + env(safe-area-inset-bottom,0px));width:92px;height:92px;background:#d62828;font-size:17px}
#b1{right:118px;bottom:calc(30px + env(safe-area-inset-bottom,0px));width:64px;height:64px;font-size:12px}
#b2{right:100px;bottom:calc(104px + env(safe-area-inset-bottom,0px));width:64px;height:64px;font-size:12px}
#b3{right:20px;bottom:calc(126px + env(safe-area-inset-bottom,0px));width:68px;height:68px;font-size:13px;background:#b8860b}
#jr{position:fixed;width:100px;height:100px;margin:-50px;border:3px solid #fff5;border-radius:50%;display:none}#jk{position:absolute;left:30px;top:30px;width:40px;height:40px;border-radius:50%;background:#fff6}
#m{position:fixed;inset:0;z-index:5;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;background:radial-gradient(#4a2a14,#0b0f1a 72%);text-align:center;padding:20px}
h1{margin:0;font-size:38px;color:var(--a);text-shadow:0 0 22px #ff7a2f88}p{margin:0;max-width:320px;opacity:.85;line-height:1.4}
input{font:inherit;border:0;border-radius:14px;padding:12px;width:250px;box-sizing:border-box;text-align:center;background:#1b2542;color:#fff;font-size:16px}.pb{position:static;border-radius:16px;width:250px;padding:14px;font-size:18px;background:var(--a);color:#2b0f00;box-shadow:0 4px 0 #8a3a00}.pb.l{background:#f2d36b}
#lb{position:fixed;inset:0;z-index:6;display:none;flex-direction:column;background:radial-gradient(#3a2414,#0b0f1a 75%);padding:max(8px,env(safe-area-inset-top)) 8px max(8px,env(safe-area-inset-bottom));box-sizing:border-box}#lt{text-align:center;padding:4px 0 8px;font-size:15px}#lw{flex:1;display:flex;gap:8px;min-height:0}
.sd{flex:1;background:#0008;border:2px solid #fff3;border-radius:14px;padding:8px;display:flex;flex-direction:column;align-items:center;gap:5px;overflow:auto;min-width:0}.sd.me{border-color:#ff7a2f}.hd{font-size:12px;opacity:.7}.nm{font-size:15px;text-align:center}.hn{font-size:16px;color:#ffd166}
.sd canvas{width:100%;max-width:150px;aspect-ratio:150/180;background:#0006;border-radius:10px}.hr{display:flex;gap:6px;flex-wrap:wrap;justify-content:center}.hr button,.dn{position:static;border-radius:10px;padding:8px 10px;font-size:13px;background:#2b3a66;box-shadow:none}.hr button.on{background:#ff7a2f;color:#2b0f00}.hr button:disabled{opacity:.35}
.sk{margin:0;padding:0;list-style:none;font-size:11px;font-weight:600;line-height:1.3;width:100%}.sk li{margin-bottom:3px}.sk b{color:#ffd166}.dn{background:#16a34a;width:100%;font-size:15px;margin-top:auto}.dn.on{background:#555}.dn:disabled{opacity:.7}
html,body{overscroll-behavior:none;position:fixed;width:100%}#rot{position:fixed;inset:0;z-index:99;background:#0b0f1a;display:none;align-items:center;justify-content:center;text-align:center;font-size:20px;padding:30px;flex-direction:column;gap:10px}@media (orientation:portrait){#rot{display:flex}}.sd canvas{height:30vh;width:auto;max-width:100%}
#cbT,#cbB{position:absolute;left:0;right:0;height:0;background:#000;transition:height .7s ease;pointer-events:none}#cbT{top:0}#cbB{bottom:0}#hud.cin #cbT,#hud.cin #cbB{height:11vh}#ms{font-size:24px}
#iv{position:absolute;top:24%;left:50%;transform:translateX(-50%);display:none;align-items:center;gap:10px;background:#000b;padding:10px 14px;border-radius:16px;pointer-events:auto;border:2px solid #b48cff;white-space:nowrap}#iv button{position:static;border-radius:12px;padding:10px 16px;font-size:15px;box-shadow:none}#ivy{background:#16a34a}#ivn{background:#b42323}
</style></head><body>
<div id="rot"><div style="font-size:54px">📱↻</div>Putar HP ke mode landscape</div>
<div id="m"><h1>Kampung Hancur</h1><p>Basmi 100 zombie bareng teman. Zombie terakhir menjatuhkan mahkota. Bos gemuk: 2 pukulannya membunuhmu.</p><input id="nm" maxlength="10" placeholder="Nama kamu"><input id="cd" maxlength="8" placeholder="Kode tim (kosong = buat baru)"><button class="pb" id="go">Masuk</button><p id="er" style="font-size:13px;color:#ffb0a0"></p></div>
<div id="lb"><div id="lt">Pilih karakter · Kode tim: <b id="lc"></b></div><div id="lw"><div class="sd" id="s1"></div><div class="sd" id="s2"></div></div><button class="pb" id="solo" style="display:none;margin:6px auto 0">Mulai sendiri</button></div>
<div id="hud"><div id="cbT"></div><div id="cbB"></div><div id="iv"><span id="ivt"></span><button id="ivy">Terima</button><button id="ivn">Tolak</button></div><div id="top"><span id="zc"></span><div class="bar"><i id="hp"></i></div></div><canvas id="rd"></canvas><div id="ms"></div>
<button id="b0">Serang</button><button id="b1"></button><button id="b2"></button><button id="b3">ULT</button></div>
<div id="jr"><div id="jk"></div></div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<script>
let R=Math.random;const T=THREE,$=i=>document.getElementById(i),PI=Math.PI,mul=a=>()=>{a|=0;a=a+0x6D2B79F5|0;let t=Math.imul(a^a>>>15,1|a);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296},rr=(a,b)=>a+R()*(b-a),cl=(v,a,b)=>Math.max(a,Math.min(b,v)),ss=x=>x*x*(3-2*x),pk=a=>a[R()*a.length|0];
const rd=new T.WebGLRenderer({antialias:false,powerPreference:'low-power'});rd.setPixelRatio(Math.min(devicePixelRatio,1.25));rd.outputEncoding=T.sRGBEncoding;document.body.prepend(rd.domElement);
const sc=new T.Scene(),cam=new T.PerspectiveCamera(60,1,.1,190);
function rs(){rd.setSize(innerWidth,innerHeight);cam.aspect=innerWidth/innerHeight;cam.updateProjectionMatrix()}addEventListener('resize',rs);rs();
R=mul(777);sc.fog=new T.Fog(0xd9946a,35,125);sc.fog.color.convertSRGBToLinear();
sc.add(new T.HemisphereLight(0xffc9a0,0x3a3a30,.9));const sun=new T.DirectionalLight(0xffb070,.9);sun.position.set(-40,30,-30);sc.add(sun);
const fl=new T.PointLight(0xffaa55,0,45);sc.add(fl);
const cv=(w,h,f)=>{const c=document.createElement('canvas');c.width=w;c.height=h;f(c.getContext('2d'),w,h);const t=new T.CanvasTexture(c);t.encoding=T.sRGBEncoding;return t};
// ===== langit
let sky;{const g=new T.SphereGeometry(170,16,10),p=g.attributes.position,a=new T.Color(0x24407a).convertSRGBToLinear(),b=new T.Color(0xf29a5a).convertSRGBToLinear(),k=new T.Color(),c=[];for(let i=0;i<p.count;i++){k.copy(b).lerp(a,Math.pow(cl(p.getY(i)/170,0,1),.5));c.push(k.r,k.g,k.b)}
g.setAttribute('color',new T.Float32BufferAttribute(c,3));sky=new T.Mesh(g,new T.MeshBasicMaterial({vertexColors:true,side:T.BackSide,fog:false,depthWrite:false}));sc.add(sky)}
const gt=cv(64,64,(g)=>{const q=g.createRadialGradient(32,32,0,32,32,32);q.addColorStop(0,'#fff');q.addColorStop(.4,'rgba(255,255,255,.5)');q.addColorStop(1,'rgba(255,255,255,0)');g.fillStyle=q;g.fillRect(0,0,64,64)});
const SP=[];function spr(x,y,z,s,col,life,gr,add,op,vx,vy,vz){let o=SP.find(q=>!q.on);if(!o){if(SP.length>240)return;o={m:new T.Sprite(new T.SpriteMaterial({map:gt,depthWrite:false,fog:false})),on:0};sc.add(o.m);SP.push(o)}
const m=o.m;m.material.color.set(col);m.material.blending=add?T.AdditiveBlending:T.NormalBlending;m.position.set(x,y,z);m.visible=true;Object.assign(o,{on:1,l:life,d:life,s,g:gr||0,op:op==null?1:op,v:[vx||0,vy||0,vz||0]});return o}
const sunS=spr(-60,40,-90,60,0xffd9a0,1e9,0,1,.9);sunS.m.material.opacity=.9;sunS.fixed=1;
const CL=[];for(let i=0;i<14;i++){const m=new T.Sprite(new T.SpriteMaterial({map:gt,color:0xf3b79a,depthWrite:false,fog:false,opacity:.5}));m.scale.set(rr(40,70),rr(10,18),1);m.position.set(rr(-150,150),rr(45,75),rr(-150,150));sc.add(m);CL.push(m)}
const RG=[];function ring(x,y,z,r,col,life){let o=RG.find(q=>!q.on);if(!o){if(RG.length>24)return;o={m:new T.Mesh(new T.RingGeometry(.85,1,28).rotateX(-PI/2),new T.MeshBasicMaterial({transparent:true,blending:T.AdditiveBlending,depthWrite:false,side:T.DoubleSide,fog:false})),on:0};sc.add(o.m);RG.push(o)}
o.m.material.color.set(col);o.m.position.set(x,y,z);o.m.visible=true;Object.assign(o,{on:1,l:life,d:life,r})}
function boom(x,y,z,s,col){spr(x,y,z,s*1.6,0xffffff,.18,.5,1);spr(x,y,z,s*3,col||0xff8a2a,.5,1.2,1,.9);ring(x,.2,z,s*3.5,col||0xffb060,.45);spr(x,y+s*.4,z,s*3,0x2a2622,1.6,1.5,0,.5,0,1.2,0);for(let i=0;i<6;i++)spr(x,y,z,s*.5,0xffd080,.7,-.5,1,1,rr(-1,1)*s*4,rr(1,4)*s,rr(-1,1)*s*4);fl.position.set(x,y+1,z);fl.intensity=3;shk=Math.max(shk,s*.12)}
// ===== bake mesh (1 draw call per bangunan)
const NI=g=>g.toNonIndexed(),GB=NI(new T.BoxGeometry(1,1,1)),GC=NI(new T.CylinderGeometry(1,1,1,8)),GT=NI(new T.CylinderGeometry(.6,1,1,7)),GK=NI(new T.ConeGeometry(1,1,7)),GK4=NI(new T.ConeGeometry(1,1,4)),GP=NI(new T.CylinderGeometry(1,1,1,3)),GS=new T.IcosahedronGeometry(1,1);
const MV=new T.MeshLambertMaterial({vertexColors:true}),M4=new T.Matrix4(),QQ=new T.Quaternion(),EE=new T.Euler(),VA=new T.Vector3(),VB=new T.Vector3(),CC=new T.Color(),N3=new T.Matrix3(),AY=new T.Vector3(0,1,0);
function Bk(){const P=[],N=[],C=[],o={a(g,col,x,y,z,sx,sy,sz,rx,ry,rz,v){M4.compose(VA.set(x,y,z),QQ.setFromEuler(EE.set(rx||0,ry||0,rz||0)),VB.set(sx,sy,sz));N3.getNormalMatrix(M4);CC.set(col).convertSRGBToLinear();const k=1+(R()-.5)*(v==null?.18:v),p=g.attributes.position,n=g.attributes.normal;
for(let i=0;i<p.count;i++){VA.fromBufferAttribute(p,i).applyMatrix4(M4);P.push(VA.x,VA.y,VA.z);VB.fromBufferAttribute(n,i).applyMatrix3(N3).normalize();N.push(VB.x,VB.y,VB.z);C.push(CC.r*k,CC.g*k,CC.b*k)}return o},
geo(){const g=new T.BufferGeometry();g.setAttribute('position',new T.Float32BufferAttribute(P,3));g.setAttribute('normal',new T.Float32BufferAttribute(N,3));g.setAttribute('color',new T.Float32BufferAttribute(C,3));g.computeBoundingSphere();return g}};return o}
function scat(geo,pts,mat){const im=new T.InstancedMesh(geo,mat||MV,pts.length),m=new T.Matrix4();pts.forEach((p,i)=>{m.compose(VA.set(p[0],p[1]||0,p[2]),QQ.setFromAxisAngle(AY,R()*6.28),VB.setScalar(p[3]||1));im.setMatrixAt(i,m)});im.frustumCulled=false;sc.add(im)}
// ===== tanah & jalan
const grT=cv(128,128,(g,w,h)=>{g.fillStyle='#4a5a2c';g.fillRect(0,0,w,h);for(let i=0;i<900;i++){g.fillStyle=`hsl(${60+R()*50},${25+R()*20}%,${18+R()*18}%)`;g.fillRect(R()*w,R()*h,2+R()*3,2+R()*3)}});grT.wrapS=grT.wrapT=T.RepeatWrapping;grT.repeat.set(70,70);
const gr=new T.Mesh(new T.PlaneGeometry(260,260),new T.MeshLambertMaterial({map:grT}));gr.rotation.x=-PI/2;sc.add(gr);
const rdT=cv(128,64,(g,w,h)=>{g.fillStyle='#3a3a3e';g.fillRect(0,0,w,h);for(let i=0;i<500;i++){g.fillStyle=`rgba(${R()*90},${R()*90},${R()*90},.5)`;g.fillRect(R()*w,R()*h,2,2)}g.fillStyle='#d8c86a';g.fillRect(0,30,50,4);g.strokeStyle='#17171a';g.lineWidth=2;for(let i=0;i<5;i++){g.beginPath();let x=R()*w,y=R()*h;g.moveTo(x,y);for(let j=0;j<6;j++)g.lineTo(x+=rr(-14,14),y+=rr(-8,8));g.stroke()}g.fillStyle='#222';g.beginPath();g.ellipse(90,14,10,5,0,0,7);g.fill()});rdT.wrapS=rdT.wrapT=T.RepeatWrapping;rdT.repeat.set(26,1);
for(const r of[0,1]){const p=new T.Mesh(new T.PlaneGeometry(220,9),new T.MeshLambertMaterial({map:rdT}));p.rotation.x=-PI/2;p.position.y=.03;if(r)p.rotation.z=PI/2;sc.add(p)}
// ===== rumah (tiap rumah beda)
const WL=[0xd8c8a8,0xb9a98a,0xc9d0c0,0xe0b8a0,0xa8b8c8,0xd0d098,0xc8a8a0],RF=[0x8a3b2a,0x5a4a44,0x3f5a4a,0x6a3a30,0x4a4a58],GR=[0x8a8478,0x6a655c,0x55504a];
function mkHouse(i){let w=rr(5,9.5),d=rr(5,8);const fl=R()<.4?2:1,h=2.7*fl,wc=WL[i%7],rc=RF[i%5],b=Bk(),rt=i%4,rh=rr(1.8,2.8),dm=R();if(rt==2)d=w;const ty=.5+h;
b.a(GB,0x77726a,0,.25,0,w+.5,.5,d+.5);b.a(GB,wc,0,.5+h/2,0,w,h,d);b.a(GB,0x9a8f80,0,.65,d/2+.03,w+.1,.25,.1);
if(rt==3){b.a(GB,0x8a8a88,0,ty+.15,0,w+.3,.3,d+.3);b.a(GC,0x3a5a7a,w/4,ty+1,-d/4,.8,1.4,.8)}else if(rt==2)b.a(GK4,rc,0,ty+rh/2,0,w*.78,rh,d*.78,0,PI/4);
else{b.a(GP,rc,0,ty+rh/3,0,(w+1)/1.732,d+1,rh/1.5,-PI/2);if(rt==1){b.a(GB,wc,w*.2,ty+rh*.2,d*.28,1.5,1.2,1.5);b.a(GK4,rc,w*.2,ty+rh*.2+1.05,d*.28,1.3,.9,1.3,0,PI/4)}}
if(rt!=3)b.a(GB,0x8a4a3a,w*.3,ty+rh*.5,-d*.15,.7,rh+.8,.7);
b.a(GB,0x3b2616,-w*.2,1.25,d/2+.05,1,1.5,.1);b.a(GB,0xe8e0d0,-w*.2,2.1,d/2+.04,1.3,.12,.1);
for(let f=0;f<fl;f++)for(const x of[-.42,.12,.38]){if(f==0&&x<-.3&&0)continue;const y=1.7+f*2.7,X=x*w;b.a(GB,0xe8e0d0,X,y,d/2+.03,1.15,1.15,.08);b.a(GB,R()<.2?0xe9c26a:0x1b222b,X,y,d/2+.06,.95,.95,.08);if(R()<.3)for(let k=0;k<2;k++)b.a(GB,0x6a4a2c,X,y+k*.4-.2,d/2+.1,1.2,.12,.05,0,0,rr(-.3,.3));
 for(const s of[-1,1]){b.a(GB,0xe8e0d0,s*(w/2+.03),y,x*d*.7,.08,1.1,1.1);b.a(GB,0x1b222b,s*(w/2+.06),y,x*d*.7,.08,.9,.9)}}
b.a(GB,0x7a5a3a,-w*.2,.55,d/2+.9,2.4,.1,1.8);for(const s of[-1,1])b.a(GB,0x6a4a2a,-w*.2+s*1.1,1.5,d/2+1.6,.14,2,.14);b.a(GB,rc,-w*.2,2.6,d/2+.9,2.8,.12,2.1,.12);
if(dm>.45){b.a(GB,0x1a1410,-w*.15,ty+rh*.3,d*.1,2.2,.2,2,.3,0,.2);for(let k=0;k<5;k++)b.a(GB,0x5a4430,-w*.15+rr(-1,1),ty+rh*.4+rr(0,.5),d*.1+rr(-1,1),.12,1.9,.12,rr(-.8,.8),0,rr(-.8,.8))}
if(dm>.7){b.a(GB,wc,w/2+.4,.9,d/2-1,.2,1.8,2.2,0,0,.35);for(let k=0;k<9;k++)b.a(GB,pk(GR),w/2+rr(.2,2),.2,rr(-d/2,d/2),rr(.3,1),rr(.3,.8),rr(.3,1),R(),R(),R())}
if(R()<.5)b.a(GB,0x2a2622,rr(-1,1)*w*.3,2,d/2+.07,rr(1.5,2.5),rr(1.5,3),.05);
for(let x=-w/2-1;x<=w/2+1;x+=1.2)if(R()>.25){b.a(GB,0x8a7458,x,.55,d/2+3.3,.12,rr(.7,1.1),.12,0,0,R()<.25?rr(-.6,.6):0)}b.a(GB,0x8a7458,0,.7,d/2+3.3,w+2,.08,.06,0,0,.03);
for(let k=0;k<3;k++)b.a(GB,0x8a6a40,rr(-w,w),.25,-d/2-rr(1,2.5),.6,.5,.6,0,R(),0);
const m=new T.Mesh(b.geo(),MV),rb=Bk();for(let k=0;k<22;k++)rb.a(GB,pk([...GR,wc,0x5a4430]),rr(-w/2,w/2),rr(.15,.5),rr(-d/2,d/2),rr(.5,1.6),rr(.3,.9),rr(.5,1.6),R(),R(),R());
const rub=new T.Mesh(rb.geo(),MV);rub.visible=false;return{m,rub,w,d,rad:Math.max(w,d)*.55+.4}}
const H=[];for(let n=0,t=0;H.length<30&&t<500;t++){const x=rr(-92,92),z=rr(-92,92);if(Math.abs(x)<8||Math.abs(z)<8||Math.hypot(x,z)<14||H.some(h=>Math.hypot(h.x-x,h.z-z)<17))continue;
const h=mkHouse(n++);h.x=x;h.z=z;h.hp=100;h.ry=Math.abs(x)<Math.abs(z)?(x>0?-PI/2:PI/2):(z>0?PI:0);for(const o of[h.m,h.rub]){o.position.set(x,0,z);o.rotation.y=h.ry+rr(-.12,.12);sc.add(o)}H.push(h)}
// ===== pohon, batu, mobil, lampu
function mkTree(k){const b=Bk(),g=[0x2f6a2c,0x3b7a30,0x4a8a38,0x5a8f3a,0x2a5a2a];
if(k==0){const Hh=rr(6,9);b.a(GT,0x4a3322,0,Hh*.25,0,.28,Hh*.5,.28);for(let i=0;i<6;i++){const s=2.1-i*.3;b.a(GK,g[i%3],rr(-.1,.1),Hh*.3+i*Hh*.12,rr(-.1,.1),s,Hh*.26,s,rr(-.08,.08),R()*6,rr(-.08,.08),.35)}}
else if(k==1){const Hh=rr(3,4.5);b.a(GT,0x5a4030,0,Hh/2,0,.35,Hh,.35,rr(-.08,.08),0,rr(-.08,.08));b.a(GB,0x5a4030,.5,Hh*.8,0,1.2,.18,.18,0,0,.5);b.a(GB,0x5a4030,-.5,Hh*.7,.2,1.1,.16,.16,0,0,-.5);for(let i=0;i<7;i++)b.a(GS,g[i%5],rr(-1.6,1.6),Hh+rr(-.2,1.4),rr(-1.6,1.6),rr(1,1.7),rr(.8,1.3),rr(1,1.7),R(),R(),0,.3)}
else if(k==2){const Hh=rr(3,5);b.a(GT,0x4a4038,0,Hh/2,0,.25,Hh,.25,0,0,rr(-.1,.1));for(let i=0;i<6;i++){const a=R()*6,L=rr(1,2);b.a(GB,0x4a4038,Math.cos(a)*.5,Hh*.6+i*.25,Math.sin(a)*.5,.1,L,.1,Math.sin(a)*.9,0,-Math.cos(a)*.9)}}
else for(let i=0;i<4;i++)b.a(GS,g[i],rr(-.7,.7),.45,rr(-.7,.7),rr(.6,1),rr(.45,.7),rr(.6,1),0,0,0,.3);return b.geo()}
{const V=[[0,0,0],[1,1,1],[2,2],[3,3,3,3]].map((a,k)=>a.map(()=>mkTree(k))),pts=V.map(a=>a.map(()=>[]));
const cnt=[35,45,22,40];for(let k=0;k<4;k++)for(let n=0;n<cnt[k];n++){let x,z,t=0;do{x=rr(-96,96);z=rr(-96,96)}while(t++<20&&(Math.abs(x)<6||Math.abs(z)<6||H.some(h=>Math.hypot(h.x-x,h.z-z)<h.rad+2.5)));pk(pts[k]).push([x,0,z,rr(.8,1.4)])}
V.forEach((a,k)=>a.forEach((g,j)=>pts[k][j].length&&scat(g,pts[k][j])));
const rk=Bk();for(let i=0;i<4;i++)rk.a(GS,pk(GR),rr(-.3,.3),.3,rr(-.3,.3),rr(.4,1),rr(.3,.7),rr(.4,1),R(),R(),0,.3);const rp=[];for(let i=0;i<70;i++)rp.push([rr(-96,96),0,rr(-96,96),rr(.6,1.8)]);scat(rk.geo(),rp);
const sc2=Bk();sc2.a(GB,0x2a2622,0,.02,0,3,.04,3);const st=[];for(let i=0;i<24;i++)st.push([rr(-90,90),0,rr(-90,90),rr(.8,2)]);scat(sc2.geo(),st)}
const FR=[];for(let i=0;i<9;i++){const b=Bk(),c=pk([0x8a2a2a,0x2a4a7a,0x6a6a6a,0x4a4a2a]);b.a(GB,c,0,.6,0,1.9,.7,4.2);b.a(GB,0x2a3038,0,1.2,-.2,1.6,.6,2.2);for(const x of[-1,1])for(const z of[-1.4,1.4])b.a(GC,0x111111,x,.3,z,.35,.3,.35,0,0,PI/2);if(R()<.5)b.a(GB,0x2a2622,0,.97,1.3,1.8,.05,1.2);const m=new T.Mesh(b.geo(),MV);const x=i<4?rr(-3,3):rr(-90,90),z=i<4?rr(-80,80):rr(-3,3);m.position.set(x,0,z);m.rotation.set(0,i<4?0:PI/2+rr(-.3,.3),rr(-.12,.12));sc.add(m);if(i<3)FR.push([x,z+.5])}
for(let i=0;i<16;i++){const b=Bk();b.a(GC,0x333333,0,2.2,0,.08,4.4,.08);b.a(GB,0x333333,.5,4.3,0,1,.08,.08);b.a(GB,0xe9c26a,.9,4.2,0,.3,.1,.2);const m=new T.Mesh(b.geo(),MV),x=pk([-5.5,5.5]),z=rr(-90,90);m.position.set(i%2?x:z,0,i%2?z:x);m.rotation.z=R()<.3?rr(-.35,.35):0;sc.add(m)}
const fires=FR.concat(H.slice(0,5).map(h=>[h.x+1,h.z]));const FS=fires.map(f=>{const a=spr(f[0],1,f[1],3.2,0xff6a1a,1e9,0,1,.9);a.fixed=1;const b2=spr(f[0],2,f[1],2,0xffd060,1e9,0,1,.9);b2.fixed=1;return[a,b2,f]});
R=Math.random;
// ===== rig karakter (tulang): hips>spine>head, bahu>siku, paha>lutut
function rig(sp){const ch=sp.ch||(sp.ch=[]);let n=0;const fw=sp.fw||1,mk=(p,x,y,z)=>{const g=new T.Group();g.position.set(x,y,z);p.add(g);return g},
P=(bone,fn)=>{const i=n++;let G=ch[i];if(!G){const b=Bk();fn(b);G=ch[i]=b.geo()}bone.add(new T.Mesh(G,MV))};
const root=new T.Group(),hips=mk(root,0,.95,0),sp1=mk(hips,0,.1,0),head=mk(sp1,0,.58,0),sh=[-1,1].map(s=>mk(sp1,s*.27*fw,.5,0)),el=sh.map(s=>mk(s,0,-.28,0)),hp=[-1,1].map(s=>mk(hips,s*.1,0,0)),kn=hp.map(s=>mk(s,0,-.46,0));
P(hips,b=>{b.a(GB,sp.bot,0,.02,0,.34*fw,.2,.2*fw);sp.hp&&sp.hp(b)});P(sp1,b=>{b.a(GB,sp.top,0,.3,0,.38*fw,.52,.22*fw);sp.tp&&sp.tp(b)});
P(head,b=>{b.a(GB,sp.sk,0,.17,0,.3,.3,.3);b.a(GB,0x222222,-.08,.17,.152,.05,.06,.02);b.a(GB,0x222222,.08,.17,.152,.05,.06,.02);sp.hd(b)});
sh.forEach(s=>P(s,b=>b.a(GB,sp.sl,0,-.14,0,.12,.3,.12)));el.forEach(e=>P(e,b=>{b.a(GB,sp.sl2||sp.sl,0,-.12,0,.1,.26,.1);b.a(GB,sp.sk,0,-.28,0,.09,.09,.09)}));
hp.forEach(h=>P(h,b=>b.a(GB,sp.bot,0,-.23,0,.15,.46,.16)));kn.forEach(k=>P(k,b=>{b.a(GB,sp.leg||sp.bot,0,-.22,0,.13,.44,.14);b.a(GB,sp.shoe,0,-.45,.04,.15,.08,.24)}));
root.scale.setScalar(sp.sc||1);const o={root,hips,sp1,head,sh,el,hp,kn};if(sp.xr)sp.xr(o);return o}
const GO=0xd8a82a,SPK={k:{sk:0xf6d5bf,top:0xc62828,sl:0xc62828,bot:0x6b4426,shoe:0x3b2a1a,sc:.95,
 hd(b){const h=0xe8e1d0;b.a(GB,h,0,.2,-.02,.35,.34,.35);b.a(GB,h,0,.32,.15,.34,.08,.06);for(const s of[-1,1]){b.a(GB,h,s*.2,.1,-.02,.08,.3,.12);b.a(GB,0xc62828,s*.22,.3,0,.07,.28,.14,0,0,-s*.5)}b.a(GB,0xc62828,0,.37,0,.39,.1,.37);b.a(GB,0xc62828,0,.33,0,.45,.04,.43);b.a(GB,GO,0,.37,.2,.08,.08,.02);for(const s of[-1,1])b.a(GB,0xd33a2c,s*.08,.17,.153,.05,.07,.02)},
 tp(b){b.a(GB,0xc62828,0,.04,0,.44,.16,.27);b.a(GB,0x4a2a14,0,.12,0,.4,.05,.25);b.a(GB,GO,0,.35,.12,.1,.1,.04);b.a(GB,0xd9c7a0,0,.3,-.18,.28,.3,.1)},
 xr(o){const b=Bk();b.a(GC,0x7a4a2a,0,0,0,.04,1.5,.04);b.a(GS,0xff6a3a,0,.82,0,.14,.14,.14,0,0,0,0);b.a(GB,GO,0,.7,0,.12,.06,.12);const m=new T.Mesh(b.geo(),MV);m.position.set(0,-.3,.04);o.el[1].add(m);const t=new T.Object3D();t.position.y=.85;m.add(t);o.tip=t}},
 l:{sk:0xf6d5bf,top:0xf7f4ee,sl:0xf7f4ee,bot:0xf6d5bf,leg:0xf6d5bf,shoe:0xf7f4ee,sc:1.05,
 hd(b){const h=0xe9c35a;b.a(GB,h,0,.2,-.02,.34,.34,.34);b.a(GB,h,0,.1,-.14,.34,.4,.1);b.a(GB,h,0,.32,.15,.32,.08,.06);for(const s of[-1,1]){b.a(GB,h,s*.18,.08,-.05,.07,.4,.08);b.a(GB,0xe0a020,s*.08,.17,.153,.05,.07,.02)}b.a(GB,0xffffff,.14,.3,.15,.08,.08,.04)},
 tp(b){b.a(GB,GO,0,.52,0,.3,.05,.2);b.a(GB,0xf2f0ea,0,.25,-.16,.36,.55,.04);b.a(GB,0xf7f4ee,0,.0,0,.46,.1,.26)},hp(b){b.a(GB,0xf7f4ee,0,-.12,0,.5,.3,.32);b.a(GB,GO,0,-.27,0,.52,.03,.34)}}};
const ZS=[0x4a5a3a,0x5a3a3a,0x3a4a5a,0x4a4a4a,0x6a5a3a,0x3a3a4a].map(c=>({sk:0x7fa05a,top:c,sl:c,bot:0x2b2f3a,shoe:0x1a1a1a,sc:rr(.95,1.08),hd(b){b.a(GB,0x2a2218,0,.31,-.02,.31,.1,.3);b.a(GB,0xff2a1a,-.08,.17,.154,.05,.05,.02);b.a(GB,0xff2a1a,.08,.17,.154,.05,.05,.02);b.a(GB,0x3a1a1a,0,.07,.152,.14,.04,.02)}}));
const ZB={sk:0x6f8a4a,top:0x5a2a2a,sl:0x6f8a4a,bot:0x2a2a30,shoe:0x111111,fw:1.7,sc:2.1,hd(b){b.a(GB,0xff2a1a,-.08,.17,.154,.06,.06,.02);b.a(GB,0xff2a1a,.08,.17,.154,.06,.06,.02);b.a(GB,0x1a1a1a,0,.34,0,.31,.04,.3)},tp(b){b.a(GB,0x5a2a2a,0,.05,.1,.8,.4,.34)}};
const LM=(c,e)=>new T.MeshLambertMaterial({color:c,emissive:e||0}),WH=LM(0xf4f4f4);
const WGB=new T.BoxGeometry(1,1,1),cyG={},cyl=s=>cyG[s]||(cyG[s]=new T.CylinderGeometry(.5,.5,1,s));
function WK(p,w,h,d,m,x,y,z){const o=new T.Mesh(WGB,m);o.scale.set(w,h,d);o.position.set(x,y,z);p.add(o);return o}
function WY(p,r,h,m,x,y,z,rx,rz,sg){const o=new T.Mesh(cyl(sg||14),m);o.scale.set(r*2,h,r*2);o.position.set(x,y,z);o.rotation.x=rx||0;o.rotation.z=rz||0;p.add(o);return o}
function AF(sk,ey,bl,br){const c=document.createElement('canvas');c.width=c.height=64;const g=c.getContext('2d');g.fillStyle=sk;g.fillRect(0,0,64,64);g.globalAlpha=.55;g.fillStyle=bl;[14,50].forEach(x=>{g.beginPath();g.ellipse(x,45,6,3,0,0,7);g.fill()});g.globalAlpha=1;
 [20,44].forEach(x=>{g.fillStyle='#fff';g.beginPath();g.ellipse(x,34,7,9,0,0,7);g.fill();g.fillStyle=ey;g.beginPath();g.ellipse(x,35,5.2,7.5,0,0,7);g.fill();g.fillStyle='#111';g.beginPath();g.ellipse(x,36,2.4,4,0,0,7);g.fill();g.fillStyle='#fff';g.fillRect(x-3,30,3,3);g.strokeStyle='#2a1a14';g.lineWidth=2.5;g.beginPath();g.moveTo(x-8,26);g.lineTo(x+8,26);g.stroke();g.strokeStyle=br;g.lineWidth=3;g.beginPath();g.moveTo(x-8,17);g.lineTo(x+8,16);g.stroke()});
 g.strokeStyle='#8a4a3a';g.lineWidth=2;g.beginPath();g.arc(32,50,4,.2,2.9);g.stroke();const t=new T.CanvasTexture(c);t.encoding=T.sRGBEncoding;return new T.MeshLambertMaterial({map:t})}
function human(o){const g=new T.Group(),[hw,hh,hd]=o.h,[tw,th,td]=o.t,[aw,ah,ad]=o.a,[lw,lh,ld]=o.l,P={l:[],a:[]};
 const part=(w,h,d,m,pv,x,y)=>{const q=new T.Group();q.position.set(x,y,0);const e=new T.Mesh(WGB,m);e.scale.set(w,h,d);e.position.y=pv*h/2;q.add(e);g.add(q);return q};
 [-1,1].forEach(s=>{P.l.push(part(lw,lh,ld,o.lm,-1,s*(lw/2+(o.lg||0)),lh));P.a.push(part(aw,ah,ad,o.am,-1,s*(tw/2+aw/2),lh+th-.02))});
 P.t=part(tw,th,td,o.tm,1,0,lh);P.h=new T.Group();P.h.position.y=lh+th;g.add(P.h);
 if(o.hc){WY(P.h,o.hc,hh,o.hm,0,hh/2,0,0,0,16);const f=new T.Mesh(new T.PlaneGeometry(hw*.95,hh*.95),o.fm);f.position.set(0,hh/2,o.hc+.003);P.h.add(f)}
 else{const e=new T.Mesh(WGB,o.hm);e.scale.set(hw,hh,hd);e.position.y=hh/2;P.h.add(e)}
 g.scale.setScalar(o.s||1);g.userData.P=P;o.x&&o.x(g,P);return g}
const WCH={};
WCH.lumine=()=>{const sk=LM('#f6d5bf'),wh=LM(0xf7f4ee),go=LM(0xd8a82a),hr=LM(0xe9c35a);
 return human({s:1.08,h:[.3,.3,.28],hm:[hr,hr,hr,sk,AF('#f6d5bf','#e0a020','#ffb3a7','#d9b04a'),hr],t:[.3,.5,.18],tm:wh,a:[.1,.5,.1],am:wh,l:[.12,.62,.12],lm:sk,lg:.015,
  x:(g,P)=>{WK(P.t,.46,.24,.28,wh,0,.02,0);WK(P.t,.48,.03,.3,go,0,-.1,0);WK(P.t,.3,.05,.2,go,0,.48,0);WK(P.t,.12,.05,.02,go,0,.3,.1);
   WK(P.h,.35,.36,.1,hr,0,.1,-.17);WK(P.h,.32,.1,.3,hr,0,.33,0);WK(P.h,.32,.09,.06,hr,0,.27,.14);[-1,1].forEach(s=>{WK(P.h,.07,.4,.08,hr,s*.18,.08,-.05);WK(P.l[s>0?1:0],.15,.14,.15,wh,0,-.55,0);WK(P.a[s>0?1:0],.12,.08,.12,go,0,-.46,0)});WK(P.h,.08,.08,.04,WH,.14,.3,.14);WK(P.h,.04,.04,.05,go,.14,.3,.15)}})};
WCH.klee=()=>{const sk='#f6d5bf',hr=LM(0xe8e1d0),rd=LM(0xc62828),go=LM(0xd8a82a);
 return human({s:.95,h:[.4,.38,.36],hm:[hr,hr,hr,LM(sk),AF(sk,'#d33a2c','#ff9a8a','#cfc7b4'),hr],t:[.3,.34,.2],tm:rd,a:[.1,.34,.1],am:rd,l:[.12,.32,.12],lm:LM(0x6b4426),lg:.02,
  x:(g,P)=>{WK(P.h,.46,.14,.42,rd,0,.44,0);WK(P.h,.52,.04,.48,rd,0,.37,0);[-1,1].forEach(s=>{WK(P.h,.07,.28,.16,rd,s*.27,.3,0).rotation.z=-s*.35;WK(P.l[s>0?1:0],.14,.1,.14,LM(0x3b2a1a),0,-.3,0)});WK(P.h,.09,.09,.02,go,0,.44,.22);WK(P.t,.26,.3,.1,LM(0xd9c7a0),0,.2,-.15);WK(P.t,.34,.04,.22,go,0,.02,0)}})};

WCH.aether=()=>{const sk=LM('#f6d5bf'),wh=LM(0xf2f2f4),gr=LM(0xc9ccd3),dk=LM(0x2b2b33),go=LM(0xd8a82a),hr=LM(0x1b1b22);
 return human({s:1.1,h:[.36,.34,.3],hm:[hr,hr,hr,sk,AF('#f6d5bf','#2a2a36','#ffb3a7','#15151a'),hr],t:[.32,.42,.2],tm:wh,a:[.1,.4,.1],am:wh,l:[.13,.5,.13],lm:gr,lg:.015,
  x:(g,P)=>{WK(P.t,.36,.08,.24,gr,0,.4,0);WK(P.t,.28,.16,.08,wh,0,.32,-.14);WK(P.t,.02,.3,.01,gr,0,.22,.105);WK(P.t,.34,.05,.22,dk,0,.02,0);WK(P.t,.06,.05,.02,go,0,.02,.115);
   WK(P.h,.38,.36,.1,hr,0,.14,-.17);WK(P.h,.38,.1,.34,hr,0,.35,0);WK(P.h,.38,.1,.06,hr,0,.29,.15);WK(P.h,.16,.14,.05,hr,-.1,.22,.155);
   [-1,1].forEach(s=>{WK(P.h,.06,.2,.2,hr,s*.19,.18,-.02);const i=s>0?1:0;WK(P.a[i],.11,.11,.11,sk,0,-.36,0);WK(P.a[i],.12,.06,.12,gr,0,-.3,0);WK(P.l[i],.16,.1,.24,wh,0,-.44,.03);WK(P.l[i],.15,.03,.23,dk,0,-.48,.03)});
   WK(P.h,.1,.1,.1,hr,-.1,.42,.02).rotation.z=.4;WK(P.h,.1,.12,.1,hr,.08,.43,-.03).rotation.z=-.35}})};

// ===== karakter ASLI dari warnet.py (tidak diubah) + rangka animasi
function wrig(k){const g=WCH[{k:'klee',l:'lumine',a:'aether'}[k]](),Q=g.userData.P,dm=()=>new T.Group(),lh=Q.l[0].position.y,tm=Q.t.children[0],th=tm.scale.y,s=g.scale.y;
 for(const a of Q.a){a.position.y=th-.02;Q.t.add(a)}Q.h.position.set(0,th,0);Q.t.add(Q.h);
 const root=new T.Group(),hips=new T.Group();hips.position.y=lh*s;g.position.y=-lh*s;hips.add(g);root.add(hips);const o={root,hips,sp1:Q.t,head:Q.h,sh:Q.a,el:[dm(),dm()],hp:Q.l,kn:[dm(),dm()],h0:lh*s};
 const ah=Q.a[1].children[0].scale.y;if(k=='k'){const pv=new T.Group();pv.position.set(0,-ah,.03);Q.a[1].add(pv);WY(pv,.022,1.3,LM(0x7a4a2a),0,.3,0);WY(pv,.05,.07,LM(0xd8a82a),0,.78,0);const orb=new T.Mesh(new T.SphereGeometry(.09,8,6),new T.MeshBasicMaterial({color:0xff6a3a}));orb.position.y=.93;pv.add(orb);const t=new T.Object3D();t.position.y=.93;pv.add(t);o.tip=t;o.stf=pv}else{const t=new T.Object3D();t.position.set(0,-ah,.03);Q.a[1].add(t);o.tip=t}
 return o}
// ===== animasi mulus (keyframe + easing)
const K={a0:[[0,-.2,0,-.3,0,0],[.3,-2.3,-.4,-1.2,.8,-.1],[.52,-.6,.5,-.2,-.8,.35],[1,-.2,0,-.3,0,0]],a1:[[0,-.2,0,-.3,0,0],[.28,-1.4,.9,-.8,-.9,0],[.5,-1.2,-.5,-.3,.9,.2],[1,-.2,0,-.3,0,0]],
a2:[[0,-.2,0,-.3,0,0],[.38,-3,0,-1,0,-.35],[.58,-.4,0,-.1,0,.65],[.75,-.4,0,-.1,0,.55],[1,-.2,0,-.3,0,0]],cs:[[0,0,0,-.3,0,0],[.35,-2.7,.7,-.6,0,-.15],[.6,-1.5,.2,-.2,0,.2],[1,0,0,-.3,0,0]],za:[[0,-1.3,0,-.2,0,.2],[.3,-2.4,0,-.4,0,-.1],[.5,-.5,0,-.1,0,.6],[1,-1.3,0,-.2,0,.2]]};
function kf(a,t){let i=0;while(i<a.length-2&&t>a[i+1][0])i++;const p=a[i],q=a[i+1],u=ss(cl((t-p[0])/(q[0]-p[0]),0,1));return p.map((v,j)=>v+(q[j]-v)*u)}
K.hf=K.hr=[[0,-2.1,0,-.25,0,0],[1,-2.1,0,-.25,0,0]];
function mkE(r,x,z){const e={r,x,z,y:0,ry:0,spd:0,ph:R()*6,fly:0,hy:0,an:null};sc.add(r.root);return e}
function pose(e,dt){const r=e.r,k=Math.min(1,dt*20),s=Math.sin(e.ph),c=Math.cos(e.ph),mv=cl(e.spd/1.2,0,1),run=cl((e.spd-3)/3.5,0,1),f=e.fly,T0=performance.now()/1000,A=mv*(.5+.45*run)*(e.zb&&!e.rn?.8:1);
const S=(o,p,t)=>{o.rotation[p]+=(t-o.rotation[p])*k};e.ph+=dt*(4.5+e.spd*(e.zb?1.1:.95));
let sh0=[s*A*.9,-.08],sh1=[-s*A*.9,.08],el0=-(.15+.7*run)*mv-.1,el1=el0,spy=s*.12*mv*(1+run),spx=(.04+.22*run)*mv+f*.1,lg=0;
if(e.zb&&!e.an){const w=Math.sin(T0*2.3+e.ph)*.12;sh0=[-1.35+w,-.1];sh1=[-1.35-w,.1];el0=el1=-.25;spx=.25+.1*mv;r.head.rotation.z=Math.sin(T0*1.7+e.x)*.2}
if(f>.01){if(e.h=='k'){sh0=[-.5*f+sh0[0]*(1-f),-1.2*f];sh1=[-1.55*f+sh1[0]*(1-f),.05*f]}else{sh0=[-2.6*f+sh0[0]*(1-f),-.5*f];sh1=[-2.6*f+sh1[0]*(1-f),.5*f]}}
const a=e.an;if(a){const v=kf(K[a.k],cl(a.t,0,1)),both=a.k=='cs'||a.k=='za';sh1=[v[0],v[1]];el1=v[2];if(both){sh0=[v[0],-v[1]];el0=v[2]}else sh0=[-v[0]*.3+.2,-.1];if(a.k=='hr'){sh0=[v[0],-v[1]];el0=v[2];sh1=[.15,.1];el1=-.2}spy=v[3];spx+=v[4];if(a.c){sh0=[a.c[0],a.c[1]];sh1=[a.c[2],a.c[3]];spy=a.c[4];spx+=a.c[5]}}
S(r.sh[0],'x',sh0[0]);S(r.sh[0],'z',sh0[1]-.05);S(r.sh[1],'x',sh1[0]);S(r.sh[1],'z',sh1[1]+.05);S(r.el[0],'x',el0);S(r.el[1],'x',el1);S(r.sp1,'y',spy);S(r.sp1,'x',spx);S(r.hips,'y',-spy*.7*(a?1:.8));S(r.hips,'x',f*.2);if(r.stf)S(r.stf,'x',e.stfT||0);if(!e.zb)S(r.head,'x',-spx*.6-f*.2);
S(r.hp[0],'x',-s*A*(1-f)+.2*f);S(r.hp[1],'x',s*A*(1-f)+.2*f);S(r.kn[0],'x',Math.max(0,c)*(.5+.8*run)*mv*(1-f)+.5*f);S(r.kn[1],'x',Math.max(0,-c)*(.5+.8*run)*mv*(1-f)+.5*f);
r.hips.position.y=(r.h0||.95)+Math.abs(c)*(.025+.05*run)*mv-.02*mv+Math.sin(T0*2+e.ph)*.006+(f>0?Math.sin(T0*3)*.05*f:0);
const d=((e.ry-r.root.rotation.y+PI)%(2*PI)+2*PI)%(2*PI)-PI;r.root.rotation.y+=d*Math.min(1,dt*(a?18:11));r.root.position.set(e.x,e.y+e.hy,e.z)}
function stepAn(e,dt){const a=e.an;if(!a)return;a.t+=dt/a.d;if(!a.done&&a.t>=a.hit){a.done=1;a.cb&&a.cb()}if(a.t>.28&&a.t<.56&&a.k[0]=='a'&&!e.zb&&!e.rem){e.x+=Math.sin(e.ry)*dt*3.2;e.z+=Math.cos(e.ry)*dt*3.2}if(a.t>=1)e.an=null}
// ===== game state (multiplayer)
const HM={k:{n:['Bunshin','Kupu²']},l:{n:['Topan','Dinding']},a:{n:['Bayangan','Sambaran']}},r2=v=>Math.round(v*100)/100;
let me=null,P={},Z=[],C=[],kills=0,boss=null,shk=0,msgT=0,won=0,yaw=0,pit=.35,TW=[],WV=[],FX=[],crown=null,ws=null,myid=0,HOST=false,room='',myn='',myh='k',snapT=0,slowT=0,sendT=0;
const F=(l,fn)=>FX.push({l,d:l,fn});
function msg(t){const m=$('ms');m.textContent=t;m.style.opacity=1;msgT=2.5}
function pushOut(o,rad){for(const h of H){if(h.hp<=0)continue;const dx=o.x-h.x,dz=o.z-h.z,dd=Math.hypot(dx,dz),m=h.rad+rad;if(dd<m&&dd>0){o.x=h.x+dx/dd*m;o.z=h.z+dz/dd*m}}o.x=cl(o.x,-98,98);o.z=cl(o.z,-98,98)}
function alive(){return Z.filter(z=>z.st!='d')}
const ang=(a,b)=>{const d=((a-b+PI)%(2*PI)+2*PI)%(2*PI)-PI;return Math.abs(d)};
function near(x,z,r,fn){for(const q of Z)if(q.st!='d'){const dx=q.x-x,dz=q.z-z,d=Math.hypot(dx,dz);if(d<r)fn(q,d,dx,dz)}}
function net(o,to){if(ws&&ws.readyState==1){if(to)o.to=to;ws.send(JSON.stringify(o))}}
function label(t){const c=cv(256,64,g=>{g.font='bold 34px sans-serif';g.textAlign='center';g.lineWidth=6;g.strokeStyle='#000';g.strokeText(t,128,44);g.fillStyle='#9fe870';g.fillText(t,128,44)});const s=new T.Sprite(new T.SpriteMaterial({map:c,depthTest:false,fog:false}));s.scale.set(3,.75,1);s.position.y=2.1;return s}
function addPlayer(id,n,h,self){if(P[id])return P[id];const e=mkE(wrig(h),0,8);Object.assign(e,{id,n,h,hp:100,cd:[0,0,0,0],cb:0,cbt:0,inv:0,u:null,wall:null,down:0,tx:0,tz:8,vx:0,vz:0,thy:0,tfy:0,rem:!self});e.x=rr(-2,2);e.z=8;if(!self)e.r.root.add(label(n));P[id]=e;return e}
function tgt(q){let b=null,bd=1e9;for(const id in P){const p=P[id];if(p.hp<=0)continue;const d=Math.hypot(p.x-q.x,p.z-q.z);if(d<bd){bd=d;b=p}}return[b,bd]}
// ===== damage (hanya host yang menghitung, hasil dikirim ke semua)
function hurt(q,d,dx,dz){if(!HOST||q.st=='d')return;q.hp-=d;const L=Math.hypot(dx,dz)||1,kb=q.bs?.15:1;q.x+=dx/L*.7*kb;q.z+=dz/L*.7*kb;spr(q.x,1.3,q.z,1.1,0xc8ff90,.25,1,1);net({t:'h',x:r2(q.x),z:r2(q.z)});if(q.hp<=0)kill(q,dx/L,dz/L,0)}
function kill(q,dx,dz,up,sh,bf){if(!HOST||q.st=='d')return;const vx=dx*(3+R()*3),vz=dz*(3+R()*3),vy=up||3;net({t:'k',i:q.i,vx,vz,vy,sh,bf});applyKill(q,vx,vz,vy,sh,bf)}
function applyKill(q,vx,vz,vy,sh,bf){if(!q||q.st=='d')return;q.st='d';q.dt=0;q.vx=vx;q.vz=vz;q.vy=vy;q.sp2=rr(-5,5);q.sh=sh;q.an=null;kills++;
 if(bf){if(bf==2&&DUOA)duoSoul(q);else{butter(q.x,1,q.z);spr(q.x,1.2,q.z,3,0xffa6e8,.5,2,1)}q.r.root.visible=false;q.dt=99;q.y=0;q.vy=0;q.gone=1;sc.remove(q.r.root)}
 if(HOST&&!alive().length)drop(q.x,q.z)}
function mkCrown(x,z){const b=Bk();b.a(GC,0xffd23f,0,.1,0,.5,.25,.5);for(let i=0;i<5;i++){const a=i*1.256;b.a(GK4,0xffd23f,Math.sin(a)*.4,.35,Math.cos(a)*.4,.14,.4,.14)}b.a(GS,0xff3355,0,.35,0,.1,.1,.1);crown=new T.Mesh(b.geo(),MV);crown.position.set(x,1,z);crown.scale.setScalar(1.8);sc.add(crown);spr(x,1.5,z,12,0xffd23f,2,0,1);msg('Zombie terakhir tumbang! Ambil mahkotanya')}
function drop(x,z){mkCrown(x,z);net({t:'crown',x,z})}
function win(){if(won)return;won=2;msg('MENANG! 👑 Semua zombie sudah dibasmi');boom(me.x,2,me.z,4,0xffd23f)}
function dmgMe(d){if(me.hp<=0||me.u||me.inv>0)return;me.hp-=d;shk=.5;spr(me.x,1.5,me.z,2,0xff3030,.3,1,1);if(me.hp<=0){me.hp=0;me.down=5;me.an=null;msg('Kamu tumbang... bangkit 5 detik lagi')}}
function hitP(p,d){if(p==me)dmgMe(d);else net({t:'dmg',d},p.id)}

// ===== chunks (tubuh zombie hancur)
const CH=new T.InstancedMesh(GB,new T.MeshLambertMaterial(),150),CD=[];CH.frustumCulled=false;sc.add(CH);for(let i=0;i<150;i++){CD.push({on:0,p:new T.Vector3(),v:new T.Vector3(),r:new T.Vector3(),s:0,l:0});CH.setMatrixAt(i,new T.Matrix4().makeScale(0,0,0));CH.setColorAt(i,CC.set(0x7fa05a))}
function shatter(q){q.r.root.visible=false;spr(q.x,1,q.z,3,0xc8ff90,.5,2,1);const cs=[0x7fa05a,q.top||0x4a4a4a,0x2b2f3a];let n=0;for(const o of CD){if(o.on)continue;o.on=1;o.l=5;o.s=rr(.18,.4)*(q.bs?2:1);o.p.set(q.x+rr(-.3,.3),q.y+rr(.5,1.5),q.z+rr(-.3,.3));o.v.set(q.vx*.5+rr(-4,4),rr(3,8),q.vz*.5+rr(-4,4));o.r.set(rr(-6,6),rr(-6,6),rr(-6,6));CH.setColorAt(CD.indexOf(o),CC.set(cs[n%3]));n++;if(n>9)break}CH.instanceColor.needsUpdate=true}
const DM=new T.Object3D();function updCh(dt){for(let i=0;i<150;i++){const o=CD[i];if(!o.on)continue;o.l-=dt;o.v.y-=22*dt;o.p.addScaledVector(o.v,dt);if(o.p.y<o.s/2){o.p.y=o.s/2;o.v.y*=-.35;o.v.x*=.7;o.v.z*=.7;o.r.multiplyScalar(.6)}DM.position.copy(o.p);DM.rotation.x+=o.r.x*dt;DM.rotation.y+=o.r.y*dt;const sc3=o.l<.8?o.s*o.l/.8:o.s;if(o.l<=0)o.on=0;DM.scale.setScalar(o.on?sc3:0);DM.updateMatrix();CH.setMatrixAt(i,DM.matrix);DM.rotation.set(0,0,0)}CH.instanceMatrix.needsUpdate=true}
// ===== kupu-kupu (1 draw call)
const BT=new T.InstancedMesh(new T.PlaneGeometry(.5,.35),new T.MeshBasicMaterial({side:T.DoubleSide,fog:false}),120),BD=[];BT.frustumCulled=false;sc.add(BT);for(let i=0;i<120;i++){BD.push({on:0,p:new T.Vector3(),a:0,ph:0,l:0});BT.setMatrixAt(i,new T.Matrix4().makeScale(0,0,0));BT.setColorAt(i,CC.set(0xff88cc))}
function butter(x,y,z){const cs=[0xff6fb5,0x6fd8ff,0xffe36f,0xb98cff];let n=0;for(let i=0;i<120&&n<5;i++){const o=BD[i];if(o.on)continue;o.on=1;o.l=rr(5,8);o.p.set(x+rr(-.4,.4),y+rr(0,1.6),z+rr(-.4,.4));o.a=R()*6.28;o.ph=R()*6;BT.setColorAt(i,CC.set(pk(cs)));n++}BT.instanceColor.needsUpdate=true}
function updBt(dt,T0){for(let i=0;i<120;i++){const o=BD[i];if(!o.on)continue;o.l-=dt;o.a+=Math.sin(T0*2+o.ph)*dt*2;o.p.x+=Math.cos(o.a)*dt*1.6;o.p.z+=Math.sin(o.a)*dt*1.6;o.p.y+=dt*(.7+Math.sin(T0*3+o.ph)*.5);DM.position.copy(o.p);DM.rotation.set(1.2,0,o.a);DM.scale.set(o.l>0?Math.abs(Math.sin(T0*14+o.ph))*1+.15:0,o.l>0?Math.min(1,o.l):0,1);if(o.l<=0)o.on=0;DM.updateMatrix();BT.setMatrixAt(i,DM.matrix)}DM.rotation.set(0,0,0);BT.instanceMatrix.needsUpdate=true}

function mkZ(t,x,z){const b=t=='b',e=mkE(rig(b?ZB:pk(ZS)),x,z);Object.assign(e,{zb:1,t,st:'a',hp:b?700:t=='r'?45:70,sp:b?1.7:t=='r'?4.2:rr(1.5,2.2),cd:rr(0,1),wt:0,bs:b,rn:t=='r',dm:b?50:8,top:0x4a4a4a,id:R()*6});if(t=='r')e.r.root.scale.multiplyScalar(.95);return e}

function zan(q,d,h,cb){q.an={k:'za',t:0,d,hit:h,cb};net({t:'za',i:q.i,ry:r2(q.ry),d})}
function zup(q,dt){const[t,d]=tgt(q);q.cd-=dt;const wk=q.wr&&q.hs&&q.hs.hp>0;if(d>=80&&!wk){q.spd*=.9;return}
let sp=0,tx=q.x,tz=q.z,ar=q.bs?3:1.7;
if(wk&&d>24){const hx=q.hs.x-q.x,hz=q.hs.z-q.z,hd=Math.hypot(hx,hz);if(hd>q.hs.rad+1){sp=q.sp;tx=q.hs.x;tz=q.hs.z}else if(q.cd<=0&&!q.an){q.ry=Math.atan2(hx,hz);q.cd=1.1;zan(q,.8,.5,()=>{q.hs.hp-=6;spr(q.hs.x+rr(-2,2),rr(1,3),q.hs.z+rr(-2,2),2.5,0x8a7a6a,1,1.5,0,.6,0,1,0)})}}
else if(t&&d<48){const dx=t.x-q.x,dz=t.z-q.z;tx=t.x;tz=t.z;if(d>ar)sp=q.sp*(d<10?1.15:1);else if(q.cd<=0&&!q.an){q.ry=Math.atan2(dx,dz);q.cd=q.bs?1.7:1.2;zan(q,q.bs?1:.75,.5,()=>{if(Math.hypot(t.x-q.x,t.z-q.z)<ar+.8)hitP(t,q.dm)})}}
else{q.wt-=dt;if(q.wt<=0){q.wt=rr(3,7);q.wx=q.x+rr(-14,14);q.wz=q.z+rr(-14,14)}tx=q.wx||q.x;tz=q.wz||q.z;if(Math.hypot(tx-q.x,tz-q.z)>1)sp=.8}
if(sp){const L=Math.hypot(tx-q.x,tz-q.z)||1;if(!q.an||q.an.t<.2||q.an.t>.7)q.ry=Math.atan2(tx-q.x,tz-q.z);q.x+=(tx-q.x)/L*sp*dt;q.z+=(tz-q.z)/L*sp*dt}
q.spd+=(sp-q.spd)*Math.min(1,dt*6);pushOut(q,.5)}
// ===== pemain & skill (p = siapa yang memakai; semua klien menjalankan visual, host yang menghitung damage)
function tipPos(p){if(p.r.tip){const v=new T.Vector3();p.r.tip.getWorldPosition(v);return v}return new T.Vector3(p.x+Math.sin(p.ry)*1.2,1.4,p.z+Math.cos(p.ry)*1.2)}
function aim(p){let b=null,bd=9;for(const q of Z)if(q.st=='a'){const d=Math.hypot(q.x-p.x,q.z-p.z);if(d<bd){bd=d;b=q}}if(b)p.ry=Math.atan2(b.x-p.x,b.z-p.z)}
function trail(e){const a=e.an;if(!a||a.k[0]!='a'||a.t<.15||a.t>.62||!e.r.tip)return;const tp=tipPos(e),L=e.h!='k';spr(tp.x,tp.y,tp.z,L?1:1.2,L?(e.h=='a'?0xc0a0ff:0xcfefff):0xff8a3a,.28,-.5,1,.85)}
function doAtk(p,c){p.cb=c+1;p.cbt=1.1;const dmg=[28,28,48][c];p.an={k:'a'+c,t:0,d:[.6,.6,.85][c],hit:[.5,.48,.58][c],cb:()=>{const tp=tipPos(p),fx=Math.sin(p.ry),fz=Math.cos(p.ry);
 if(p.h=='k'){spr(tp.x,tp.y,tp.z,2.6+c,0xffffff,.15,.5,1);spr(tp.x,tp.y,tp.z,3.4+c,0xff7a3a,.4,1.4,1);ring(p.x+fx*1.8,.2,p.z+fz*1.8,2.4+c,0xffa060,.35);for(let i=0;i<5;i++)spr(tp.x,tp.y,tp.z,.5,0xffd080,.5,-.6,1,1,rr(-3,3),rr(0,3),rr(-3,3));
  near(p.x,p.z,3.6+c*.4,(q,d,dx,dz)=>{if(ang(Math.atan2(dx,dz),p.ry)<1.2)hurt(q,dmg,dx,dz)});
  if(c==2){const sx=tp.x,sy=tp.y,sz=tp.z,ex=p.x+fx*6,ez=p.z+fz*6,m=new T.Mesh(new T.SphereGeometry(.28,8,6),new T.MeshBasicMaterial({color:0xff4a1a}));sc.add(m);F(.5,pr2=>{m.position.set(sx+(ex-sx)*pr2,sy*(1-pr2)+Math.sin(pr2*PI)*2.2,sz+(ez-sz)*pr2);spr(m.position.x,m.position.y,m.position.z,.9,0xffaa44,.25,-.4,1);if(pr2>=.99){sc.remove(m);boom(ex,.5,ez,2,0xff8a2a);near(ex,ez,4,(q,d,dx,dz)=>hurt(q,35,dx,dz))}})}}
 else{const VA2=p.h=='a';spr(tp.x,tp.y,tp.z,2,VA2?0xd8c8ff:0xe8ffff,.3,1.5,1);const g=new T.Mesh(new T.RingGeometry(.7,1,16,1,-1,2).rotateX(-PI/2),new T.MeshBasicMaterial({color:VA2?0xa070ff:0xbff6ff,transparent:true,blending:T.AdditiveBlending,side:T.DoubleSide,depthWrite:false,fog:false}));g.scale.setScalar(1.6+c*.6);g.position.set(tp.x,tp.y,tp.z);g.rotation.y=p.ry-PI/2;sc.add(g);WV.push({m:g,x:tp.x,y:tp.y,z:tp.z,dx:fx,dz:fz,l:.6,dmg,hit:new Set(),sz:1.6+c*.6})}}}}
function atk(){if(!me||me.an||me.u||me.hp<=0)return;aim(me);const c=me.cbt>0?me.cb%3:0;net({t:'a',c,x:r2(me.x),z:r2(me.z),ry:r2(me.ry)});doAtk(me,c)}
function cast(p,cb){p.an={k:'cs',t:0,d:.9,hit:.45,cb}}
function doSkill(p,n){if(p.hp<=0)return;if(n==3)return ult(p);(p.h=='k'?(n==1?bunshin:kupu):p.h=='a'?(n==1?bayangan:sambaran):(n==1?topan:dinding))(p)}
let INV=null,IVR=null,DUOF=0;
function skill(n){if(!me||me.hp<=0||me.u||me.cd[n]>0||INV||(me.an&&me.an.k=='cs'))return;
 if(n==3&&me.h=='a'){const pt=Object.values(P).find(o=>o.h=='l'&&o.hp>0&&!o.u&&Math.hypot(o.x-me.x,o.z-me.z)<25);if(pt){INV={to:pt.id,t:5.5};net({t:'inv'},pt.id);msg('Mengajak Lumine... tunggu dia terima');return}}
 skillGo(n,0)}
function skillGo(n,duo){me.cd[n]=(me.h=='a'?[0,9,13,45]:[0,12,14,45])[n];net({t:'s',n,d:duo?1:0,x:r2(me.x),z:r2(me.z),ry:r2(me.ry)});DUOF=duo?1:0;doSkill(me,n);DUOF=0}
function ivClose(dec){if(!IVR)return;if(dec)net({t:'dec'},IVR.from);IVR=null;$('iv').style.display='none'}
function invUp(dt){if(INV){INV.t-=dt;if(!me||me.hp<=0||me.u)INV=null;else if(INV.t<=0){INV=null;msg('Lumine nggak jawab, ULT solo');skillGo(3,0)}}
 if(IVR){IVR.t-=dt;if(IVR.t<=0||!me||me.hp<=0||me.u)ivClose(1)}}
$('ivy').addEventListener('pointerdown',e=>{e.preventDefault();if(IVR){net({t:'acc'},IVR.from);IVR=null;$('iv').style.display='none';msg('Siap! GERHANA KEMBAR...')}});
$('ivn').addEventListener('pointerdown',e=>{e.preventDefault();ivClose(1)});
function puff(x,z){spr(x,1,z,3,0xdddddd,.7,1.5,0,.8,0,.6,0)}
function bunshin(p){cast(p,()=>{if(p==me)msg('Kagebunshin no Jutsu!');for(let i=0;i<12;i++){const a=i/12*6.28,r=2.5+R()*2,c=mkE(wrig('k'),p.x+Math.sin(a)*r,p.z+Math.cos(a)*r);c.life=18;c.t0=0;c.o=p;c.h='k';C.push(c);puff(c.x,c.z)}})}
function kupu(p){cast(p,()=>{if(p==me)msg('Kupu-kupu!');ring(p.x,.2,p.z,15,0xff9ae0,.8);spr(p.x,1,p.z,10,0xff9ae0,.6,2,1);if(HOST)near(p.x,p.z,15,(q,d,dx,dz)=>setTimeout(()=>{if(q.st=='d')return;if(q.bs)hurt(q,150,dx,dz);else kill(q,0,0,0,0,1)},d*35))})}
function topan(p){cast(p,()=>{if(p==me)msg('Angin Topan!');for(let i=-1;i<=1;i++){const a=p.ry+i*.45,m=mkTor(1);m.position.set(p.x+Math.sin(p.ry)*1.5,0,p.z+Math.cos(p.ry)*1.5);sc.add(m);TW.push({m,x:p.x+Math.sin(p.ry)*1.5,z:p.z+Math.cos(p.ry)*1.5,dx:Math.sin(a),dz:Math.cos(a),l:4.5,lift:[]})}})}
function dinding(p){cast(p,()=>{if(p==me)msg('Dinding Angin!');if(p.wall)sc.remove(p.wall.m);const t=cv(64,64,g=>{for(let i=0;i<6;i++){g.fillStyle=`rgba(200,245,255,${.25+R()*.5})`;g.fillRect(i*11,0,4+R()*4,64)}});t.wrapS=t.wrapT=T.RepeatWrapping;t.repeat.set(4,1);const m=new T.Mesh(new T.CylinderGeometry(5.6,5.6,4.5,28,1,true),new T.MeshBasicMaterial({map:t,transparent:true,blending:T.AdditiveBlending,side:T.DoubleSide,depthWrite:false,fog:false}));sc.add(m);p.wall={m,t,l:15};ring(p.x,.2,p.z,6,0xbff6ff,.6)})}
// ===== ULT sinematik
function ult(p){if(p.u&&p.u.duo)return;const pt=DUOF&&p.h=='a'?Object.values(P).find(o=>o!==p&&o.hp>0&&o.h=='l'):null;if(pt){duoStart(p,pt);return}
p.u={t:0,a:p.ry,fired:new Set(),bm:null,tw:null,bh:null};p.inv=1;p.an=null;if(p==me)msg((p.h=='k'?'ULTIMATE: Laser Penghancur!':p.h=='a'?'ULTIMATE: Singularitas!':'ULTIMATE: Badai Petir!'));
const g=new T.Group(),mt=(c,o)=>new T.MeshBasicMaterial({color:c,transparent:true,opacity:o,blending:T.AdditiveBlending,depthWrite:false,fog:false,side:T.DoubleSide});
if(p.h=='k'){const a=new T.Mesh(new T.CylinderGeometry(1,1,1,12,1,true).rotateX(PI/2).translate(0,0,.5),mt(0xff5a1a,.5)),b=new T.Mesh(new T.CylinderGeometry(.4,.4,1,10,1,true).rotateX(PI/2).translate(0,0,.5),mt(0xffffff,.95));g.add(a,b);g.visible=false;g.rotation.order='YXZ';sc.add(g);p.u.bm=g}
else if(p.h=='a'){p.u.bh=mkBH()}else{const t=mkTor(5);t.visible=false;sc.add(t);p.u.tw=t}}

function zdead(q,dt){const r=q.r.root;if(q.y>0||q.vy>0){q.vy-=24*dt;q.x+=q.vx*dt;q.z+=q.vz*dt;q.y+=q.vy*dt;r.rotation.x+=q.sp2*dt;r.rotation.z+=q.sp2*.6*dt;if(q.y<=0){q.y=0;if(q.sh){shatter(q);q.dt=3.2;q.vy=0;return}if(Math.abs(q.vy)>5){q.vy*=-.3;q.vx*=.5;q.vz*=.5;q.y=.01}else{q.vy=0;q.sp2=0}}}else{q.dt+=dt;const k=Math.min(1,dt*8);r.rotation.x+=((q.sp2>0?1.5:-1.5)-r.rotation.x)*k;r.rotation.z+=(0-r.rotation.z)*k;for(const j of[...q.r.sh,...q.r.hp])j.rotation.z+=(Math.sin(q.id+j.id)*.5-j.rotation.z)*k;q.y=.12}
r.position.set(q.x,q.y,q.z);if(q.dt>5){const u=cl((q.dt-5)/1.2,0,1);r.position.y=q.y-u*.8;r.scale.setScalar((q.bs?2.1:1)*(1-u*.5));if(u>=1){sc.remove(r);q.gone=1}}}

function mkTor(s){const g=new T.Group(),mt=new T.MeshBasicMaterial({color:0xbfefff,transparent:true,opacity:.28,blending:T.AdditiveBlending,side:T.DoubleSide,depthWrite:false,fog:false});for(let i=0;i<6;i++){const m=new T.Mesh(new T.CylinderGeometry(.6+i*.4,.4+i*.35,1.6,14,1,true),mt);m.position.y=.8+i*1.5;m.scale.setScalar(s);m.userData.sp=1+i*.3;g.add(m);g.scale.y=1}g.children.forEach((m,i)=>m.position.y*=s);return g}

function ultUp(p,dt){if(p.u.duo)return duoUp(p,dt);const u=p.u,t=u.t+=dt,K0=p.h=='k',hy=t<1.3?ss(t/1.3)*6:t<4.8?6+Math.sin(t*3)*.25:6*(1-ss(cl((t-4.8)/1.3,0,1)));p.hy=hy;p.fly=t<5.4?cl(t/.9,0,1):cl(1-(t-5.4)/.6,0,1);p.inv=1;p.spd=0;p.stfT=(K0&&t>.9&&t<5)?PI:0;
p.an=t<1.1?{k:'cs',t:cl(t/1.1*.4,0,.4),d:9,hit:9}:null;const tp=tipPos(p);
if(t<1.5){const k=t/1.5;spr(tp.x,tp.y,tp.z,.8+k*3.4,K0?0xff7a3a:0xbff6ff,.15,0,1);if(R()<.8)spr(tp.x+rr(-2,2)*(1-k),tp.y+rr(-2,2)*(1-k),tp.z+rr(-2,2)*(1-k),.5,0xffd080,.3,-.9,1)}
if(K0){p.ry=u.a+Math.max(0,t-1.5)*1.9;const g=u.bm;if(t>1.5&&t<4.8){g.visible=true;const a=p.ry,k=1+Math.sin(t*40)*.08;g.position.set(tp.x,tp.y,tp.z);g.rotation.set(Math.atan2(tp.y,45),a,0);g.children[0].scale.set(1.8*k,1.8*k,70);g.children[1].scale.set(.5,.5,70);spr(tp.x,tp.y,tp.z,4*k,0xffffff,.08,0,1);
 if(R()<.8){const D=rr(5,50);boom(tp.x+Math.sin(a)*D,.5,tp.z+Math.cos(a)*D,rr(1,2),0xff7a2a)}
 for(const q of Z)if(q.st!='d'){const dx=q.x-p.x,dz=q.z-p.z;if(Math.hypot(dx,dz)<75&&ang(Math.atan2(dx,dz),a)<.16){boom(q.x,1,q.z,1.6,0xff6a2a);kill(q,dx/10,dz/10,rr(7,12),0)}}}else g.visible=false}
else if(p.h=='a')ultA(p,dt,u,t,tp);else{p.ry+=dt*(t>1.4?2.2:0);const w=u.tw;if(t>1.4&&t<4.9){w.visible=true;w.position.set(p.x,0,p.z);const g=cl((t-1.4)/1.2,.2,1);w.scale.set(g*2.4,1,g*2.4);w.children.forEach(m=>m.rotation.y+=dt*m.userData.sp*9);
 if(R()<.4)bolt(tp.x,tp.y,tp.x+rr(-6,6),tp.z+rr(-6,6),45,tp.z);spr(tp.x,tp.y,tp.z,2.5,0xbff6ff,.1,0,1);
 if(R()<.7){const a=R()*6.28,D=rr(3,45),X=p.x+Math.sin(a)*D,Z0=p.z+Math.cos(a)*D;bolt(X+rr(-3,3),45,X,Z0,0,Z0);boom(X,.4,Z0,rr(1,1.8),0xbfd8ff)}
 for(const q of Z)if(q.st!='d'&&!u.fired.has(q)){const dx=q.x-p.x,dz=q.z-p.z,d=Math.hypot(dx,dz);if(d<75&&t>1.6+d/75*2.4){u.fired.add(q);bolt(q.x,45,q.x,q.z,1,q.z);boom(q.x,1,q.z,1.5,0xbfd8ff);if(q.bs)hurt(q,160,dx,dz);else kill(q,-dz/d*3,dx/d*3,rr(9,14),0)}}}else w.visible=false}
if(t>=6.3){if(u.bm)sc.remove(u.bm);if(u.tw)sc.remove(u.tw);if(u.bh)rmBH(u.bh);p.u=null;p.hy=0;p.fly=0;p.inv=1.5;p.stfT=0}}
function bolt(x1,y1,x2,z2,y2,z1,col){const pts=[];const n=9;for(let i=0;i<=n;i++){const u=i/n;pts.push(new T.Vector3(x1+(x2-x1)*u+(i&&i<n?rr(-1.5,1.5):0),y1+(y2-y1)*u,z1+(z2-z1)*u+(i&&i<n?rr(-1.5,1.5):0)))}const l=new T.Line(new T.BufferGeometry().setFromPoints(pts),new T.LineBasicMaterial({color:col||0xdfe8ff,blending:T.AdditiveBlending,fog:false}));sc.add(l);F(.14,(p)=>{l.material.opacity=1-p;if(p>=.99){sc.remove(l);l.geometry.dispose()}})}

// ===== AETHER: bayangan & petir | ULT solo "Singularitas" | ULT DUO "GERHANA KEMBAR" (bareng Lumine)
function bayangan(p){p.an={k:'a1',t:0,d:.5,hit:.3,cb:()=>{if(p==me)msg('Dash Bayangan!');const sx=p.x,sz=p.z,fx=Math.sin(p.ry),fz=Math.cos(p.ry),hit=new Set();p.inv=Math.max(p.inv,.6);ring(sx,.2,sz,3,0x9a6bff,.4);
 F(.28,pr=>{const e=ss(Math.min(1,pr)),x=sx+fx*11*e,z=sz+fz*11*e;if(p==me){p.x=x;p.z=z;pushOut(p,.5)}spr(x,1.1,z,1.7,0x9a6bff,.35,-.4,1,.7);spr(x,.7,z,1.2,0x15101f,.5,0,0,.6);
  near(x,z,2.3,(q,d,dx,dz)=>{if(!hit.has(q)){hit.add(q);spr(q.x,1.2,q.z,2.2,0xffffff,.2,1,1);hurt(q,q.bs?40:75,fx*3+dx*.3,fz*3+dz*.3)}})})}}}
function sambaran(p){cast(p,()=>{if(p==me)msg('Sambaran!');const fx=Math.sin(p.ry),fz=Math.cos(p.ry),px=p.x,pz=p.z;
 for(let i=0;i<6;i++)setTimeout(()=>{const d=4+i*3.4,x=px+fx*d+rr(-1,1),z=pz+fz*d+rr(-1,1);bolt(x+rr(-2,2),38,x,z,0,z,0xcdb8ff);spr(x,.6,z,5,0xb48cff,.35,1.2,1);spr(x,1.5,z,2.6,0xffffff,.2,1,1);ring(x,.2,z,4.4,0xa070ff,.4);shk=Math.max(shk,.25);near(x,z,4.4,(q,dd,dx,dz)=>hurt(q,q.bs?55:95,dx,dz))},i*120)})}
// lubang hitam (ringan: 3 mesh + 1 sprite, geometri dipakai ulang)
let BHG=null;
function mkBH(){if(!BHG)BHG={c:new T.SphereGeometry(1,12,8),d:new T.RingGeometry(1.3,2.8,28).rotateX(-PI/2),e:new T.RingGeometry(2.9,3.3,28).rotateX(-PI/2)};
 const g=new T.Group(),ad=(c,o)=>new T.MeshBasicMaterial({color:c,transparent:true,opacity:o,blending:T.AdditiveBlending,side:T.DoubleSide,depthWrite:false,fog:false});
 const d=new T.Mesh(BHG.d,ad(0xa070ff,.85)),d2=new T.Mesh(BHG.e,ad(0xffffff,.5));g.add(new T.Mesh(BHG.c,new T.MeshBasicMaterial({color:0,fog:false})),d,d2);g.visible=false;g.rotation.set(.45,0,.2);sc.add(g);
 const gl=spr(0,0,0,8,0x8050ff,1e9,0,1,.5);gl.fixed=1;gl.m.visible=false;gl.m.material.opacity=.55;return{g,d,d2,gl}}
function setBH(b,c,r,dt){const v=r>.05;b.g.visible=v;b.gl.m.visible=v;if(!v)return;b.g.position.set(c.x,c.y,c.z);b.g.scale.setScalar(r);b.d.rotation.y+=dt*4;b.d2.rotation.y-=dt*2.2;b.gl.m.position.set(c.x,c.y,c.z);b.gl.m.scale.setScalar(r*6+Math.sin(performance.now()/90)*.3)}
function rmBH(b){sc.remove(b.g);b.gl.on=0;b.gl.fixed=0;b.gl.m.visible=false}
function bhSpark(c,n){if(R()<n){const a=R()*6.28,d=rr(7,20);spr(c.x+Math.cos(a)*d,c.y+rr(-3,4),c.z+Math.sin(a)*d,.7,0xc0a0ff,.45,-.7,1,.9,-Math.cos(a)*d*2,0,-Math.sin(a)*d*2)}}
// host: sedot zombie memutar ke pusat lubang hitam
function bhPull(c,Rd,dt){for(const q of Z){if(q.st=='d'||q.gone)continue;
 if(q.st=='a'&&!q.bs){const dx=q.x-c.x,dz=q.z-c.z,d=Math.hypot(dx,dz);if(d<Rd){q.st='l';q.hb=c;q.la=Math.atan2(dz,dx);q.lr=d;q.lh=q.y||0;q.lt=0}}
 else if(q.st=='l'&&q.hb===c){q.lt+=dt;q.la+=dt*(2+q.lt*3);q.lr=Math.max(1.3,q.lr-dt*(9+q.lt*5));q.lh+=(c.y-q.lh)*Math.min(1,dt*1.8);q.x=c.x+Math.cos(q.la)*q.lr;q.z=c.z+Math.sin(q.la)*q.lr;q.y=q.lh;const r=q.r.root;r.rotation.y+=dt*9;r.rotation.z=Math.sin(q.lt*5)*.6;r.position.set(q.x,q.y,q.z);for(const j of[...q.r.sh,...q.r.hp])j.rotation.x+=(Math.sin(q.lt*10)*2-j.rotation.x)*.2}}}
function bhEnd(c,rad,bd){if(!HOST)return;let n=0;for(const q of Z){if(q.st=='d'||q.gone)continue;const dx=q.x-c.x,dz=q.z-c.z,d=Math.hypot(dx,dz)||1;
 if(q.bs){if(d<rad+20)hurt(q,bd,dx,dz);continue}if((q.st=='l'&&q.hb===c)||d<rad){q.st='a';q.hb=null;q.hp=0;kill(q,dx/d*2,dz/d*2,rr(6,12),n++<10?1:0)}}}
function ultA(p,dt,u,t,tp){const b=u.bh;if(!u.c){u.c={x:p.x+Math.sin(p.ry)*11,y:5.5,z:p.z+Math.cos(p.ry)*11}}const c=u.c;p.ry=Math.atan2(c.x-p.x,c.z-p.z);
 if(t>1.3&&t<4.4){const g=ss(cl((t-1.3)/.9,0,1)),sh=t>4.1?1-ss(cl((t-4.1)/.3,0,1)):1;setBH(b,c,g*2.4*sh,dt);if(HOST)bhPull(c,34,dt);bhSpark(c,.5);if(R()<.35)bolt(tp.x,tp.y,c.x,c.z,c.y,tp.z,0xcdb8ff)}
 else if(t>=4.4&&!u.ex){u.ex=1;setBH(b,c,0,dt);boom(c.x,c.y,c.z,4,0xb080ff);ring(c.x,.3,c.z,22,0xc0a0ff,.7);bhEnd(c,45,200)}}
// ----- ULT DUO "GERHANA KEMBAR": tatap -> tos -> terbang bareng -> serang bareng (blackhole mini) -> blackhole raksasa -> dihantam ke tanah
let DUOA=null;const MB=[],OR=[];
function duoStart(p,q){const mx=(p.x+q.x)/2,mz=(p.z+q.z)/2,dd=Math.hypot(p.x-q.x,p.z-q.z),ph0=dd>.4?Math.atan2(p.z-mz,p.x-mx):0,r0=.475,cx=Math.cos(ph0),sx=Math.sin(ph0);
 const D={t:0,ta:cl(.8+dd*.05,.8,1.9),k:0,g:0,pu:0,ex:0,f1:0,f2:0,mx,mz,ph0,phi:ph0,c:{x:mx,y:4,z:mz},y0:0,bh:mkBH(),pl:[p,q],aim:[null,null],sx:[p.x,q.x],sz:[p.z,q.z],fx:[mx+cx*r0,mx-cx*r0],fz:[mz+sx*r0,mz-sx*r0]};DUOA=D;
 [p,q].forEach((x,i)=>{const o=x.u;if(o){if(o.bm)sc.remove(o.bm);if(o.tw)sc.remove(o.tw);if(o.bh)rmBH(o.bh)}x.u={duo:1,t:0,D,o:i?p:q,i,ph:-1};x.inv=1;x.an=null;if(x==me)me.cd[3]=Math.max(me.cd[3],45)});
 if(me&&(me==p||me==q)){$('hud').classList.add('cin');msg('Tatap dulu...')}}
function duoEnd(p){const u=p.u;if(u&&u.D&&!u.D.cl){const D=u.D;D.cl=1;rmBH(D.bh);for(const m of MB){m.on=0;setBH(m.b,m,0,0)}for(const o of OR)relOrb(o);OR.length=0;if(DUOA===D)DUOA=null;
  if(HOST)for(const q of Z)if(q.st=='l'&&q.hb===D.c){q.st='a';q.hb=null;q.y=0;q.r.root.rotation.set(0,0,0)}}
 p.u=null;p.hy=0;p.fly=0;p.inv=1.5;p.an=null;p.spd=0;if(p==me)$('hud').classList.remove('cin')}
// ---- koreografi (T = waktu sinematik; T=1 saat keduanya sudah berhadapan)
const duoR=T=>{let r=.475+1.925*ss(cl((T-3)/.9,0,1));if(T>6.3)r-=.9*ss(cl((T-6.3)/.9,0,1));if(T>8.2)r+=.5*ss(cl((T-8.2)/1,0,1));return r};
const duoHy=T=>{let h=3.1*ss(cl((T-1.75)/1.4,0,1));if(T>3.2&&T<6.5)h+=Math.sin(T*2.6)*.16*ss(cl((T-3.2)/.5,0,1))*(1-ss(cl((T-6.2)/.3,0,1)));h+=3*ss(cl((T-6.3)/1.3,0,1));if(T>8.3)h*=1-ss(cl((T-8.3)/1.2,0,1));return h};
function duoPos(D,i,T){if(T<1){const k=ss(cl(D.t/D.ta,0,1));return[D.sx[i]+(D.fx[i]-D.sx[i])*k,D.sz[i]+(D.fz[i]-D.sz[i])*k]}const r=duoR(T),a=D.phi+(i?PI:0);return[D.mx+Math.cos(a)*r,D.mz+Math.sin(a)*r]}
// pose lengan [bahu kiri x,z, bahu kanan x,z, putar badan, condong]
function duoArms(T,fr,t){
 if(T<1)return null;
 if(T<1.5){const w=ss(cl((T-1.05)/.45,0,1));return[-.2-1.9*w,-(.1+.3*w),-.2-1.9*w,.1+.3*w,0,.05*w]}
 if(T<1.75){const w=ss(cl((T-1.5)/.18,0,1));return[-2.1+.5*w,-.4*(1-w),-2.1+.5*w,.4*(1-w),0,.05+.12*w]}
 if(T<3){return[-1.6,0,-1.6,0,0,.12+Math.sin(t*5)*.02]}
 if(T<6.3){const w=ss(cl((T-3)/.5,0,1));return[-1.6-1.15*w+Math.sin(T*6)*.05*w,-.3*w,-1.6+w*(-.7+.8*fr),w*.15*(1-fr),0,.12-.02*w+.15*fr*w]}
 if(T<7.55){const w=ss(cl((T-6.3)/.8,0,1)),tr=Math.sin(T*18)*.03*w;return[-2.75-.3*w+tr,-.3+.02*w,-2.3-.75*w+tr,.15+.13*w,0,-.08*w]}
 if(T<8.2){const s=ss(cl((T-7.55)/.25,0,1));return[-3.05+2*s,-.28*(1-s),-3.05+2*s,.28*(1-s),0,-.08+.55*s]}
 if(T<9.6){const r=ss(cl((T-8.2)/1,0,1));return[-1.05+.85*r,-.1*r,-1.05+.85*r,.1*r,0,.47*(1-r)]}
 return null}
function duoUp(p,dt){const u=p.u,D=u.D,o=u.o,i=u.i,A=i==0;if(!o||P[o.id]!==o||!o.u||!o.u.duo){duoEnd(p);return}
 if(A){D.t+=dt;const Tn=D.t-D.ta+1;D.phi+=2.3*ss(cl((Tn-1.9)/1,0,1))*(1-.65*ss(cl((Tn-6.3)/.9,0,1)))*dt}
 const t=D.t,T=t-D.ta+1,hy=duoHy(T),pp=duoPos(D,i,T),op=duoPos(D,1-i,T),c=D.c,col=A?0xc0a0ff:0xbff6ff;
 const mv=Math.hypot(pp[0]-p.x,pp[1]-p.z)/Math.max(dt,.001);p.x=p.tx=pp[0];p.z=p.tz=pp[1];p.vx=p.vz=0;p.spd=T<1?Math.min(mv,7):0;
 p.inv=1;p.stfT=0;p.hy=hy;p.fly+=((T>1.5&&T<9)?1-p.fly:-p.fly)*Math.min(1,dt*6);
 const ph=T<1?0:T<1.75?1:T<3.1?2:T<6.3?3:T<7.6?4:5;if(u.ph!=ph){u.ph=ph;if(!p.rem&&ph)msg(['','Tatap...','TOS!','SERBUUU!','SINGULARITAS!','✦ GERHANA KEMBAR ✦'][ph])}
 // arah hadap: saling tatap; pas nyerang menghadap target
 const ai=D.aim[i],fr=ai?cl(1-(t-ai.t)/.45,0,1):0;let ry=Math.atan2(op[0]-pp[0],op[1]-pp[1]);if(T>=3.2&&T<6.3&&ai)ry=Math.atan2(ai.x-pp[0],ai.z-pp[1]);p.ry=ry;
 const ar=duoArms(T,fr,t);p.an=ar?{k:'hf',t:.5,d:1e9,hit:1e9,c:ar}:null;
 // aura: tatap-tatapan & terbang
 if(T>1&&T<1.75&&R()<.7)spr(p.x,hy+.9+rr(0,.9),p.z,1.5+(T-1)*1.5,col,.35,.8,1,.35);
 if(T>1.1&&T<1.7&&R()<.5)spr(p.x+Math.sin(p.ry)*.18,hy+1.78,p.z+Math.cos(p.ry)*.18,.5,0xffffff,.18,1,1,.9);
 if(T>=1.75&&T<8.1&&R()<.45)spr(p.x,hy+.9,p.z,1.5,col,.4,-.4,1,.45);
 if(T>=3.4&&T<7.6&&c.y>0&&R()<.3){const tp=tipPos(p);bolt(tp.x,tp.y,c.x,c.z,c.y,tp.z,col)}
 if(A){const mx=D.mx,mz=D.mz;
  if(!D.f1&&T>=1.62){D.f1=1;spr(mx,1.8,mz,9,0xffffff,.35,1.6,1);spr(mx,1.8,mz,6,0xc0a0ff,.5,2,1,.8);spr(mx,1.8,mz,6,0xbff6ff,.5,2,1,.8);ring(mx,.3,mz,7,0xc0a0ff,.55);ring(mx,.3,mz,12,0xbff6ff,.8);shk=Math.max(shk,.55);fl.position.set(mx,2,mz);fl.intensity=4}
  if(!D.f2&&T>=3.05){D.f2=1;spr(mx,hy+1.7,mz,12,0xffffff,.4,2,1);ring(mx,.3,mz,18,0xc0a0ff,.7);ring(mx,.3,mz,10,0xbff6ff,.5);shk=Math.max(shk,.5)}
  if(T>=3.3&&T<6.3)duoTick(D,dt);
  updMini(dt);updOrbs(D,dt);
  // blackhole utama: muncul di antara tangan, membesar (makin banyak jiwa zombie masuk makin gede), diangkat, lalu dihantam
  let rb=0;if(T>=3&&T<8.1)rb=Math.min(4.4,.35+3.1*ss(cl((T-3.1)/4.2,0,1))+Math.min(D.g,30)*.04)*(1+.12*D.pu);else if(T>=8.1&&T<8.45)rb=4.6*(1-ss(cl((T-8.1)/.35,0,1)));
  D.pu*=Math.max(0,1-dt*6);
  if(T<7.55){c.x=mx;c.z=mz;c.y=hy+2.1+rb*1.05}
  else if(T<8.1){if(!D.y0)D.y0=c.y;const k=cl((T-7.6)/.5,0,1);c.y=D.y0+(1.6-D.y0)*k*k;rb*=1+.1*k;if(R()<.7)spr(c.x+rr(-2,2),c.y+rr(0,3),c.z+rr(-2,2),2.4,0xb48cff,.35,-.5,1,.7)}
  setBH(D.bh,c,rb,dt);if(rb>.3)bhSpark(c,.8);
  if(HOST&&T>=6&&T<8.1)bhPull(c,78,dt);
  if(T>=6.4&&T<7.55)shk=Math.max(shk,.1+.1*ss(cl((T-6.4)/1,0,1)));
  if(!D.ex&&T>=8.1){D.ex=1;boom(c.x,1.5,c.z,7,0xb080ff);spr(c.x,2,c.z,50,0xffffff,.5,1.5,1);ring(c.x,.3,c.z,32,0xc0a0ff,.9);ring(c.x,.3,c.z,58,0xffffff,1.2);ring(c.x,.3,c.z,88,0xbff6ff,1.5);shk=1.8;fl.position.set(c.x,4,c.z);fl.intensity=7;bhEnd(c,88,380)}}
 if(T>=9.7)duoEnd(p)}
// host: tiap tembakan, 2 zombie dihisap blackhole mini (visual jalan di semua client lewat applyKill -> duoSoul)
function duoTick(D,dt){if(!HOST)return;D.k-=dt;if(D.k>0)return;D.k=.22;
 const L=Z.filter(z=>z.st=='a'&&!z.bs&&Math.hypot(z.x-D.mx,z.z-D.mz)<44);
 for(let i=0;i<2&&L.length;i++){const q=L.splice(R()*L.length|0,1)[0],dx=q.x-D.mx,dz=q.z-D.mz,d=Math.hypot(dx,dz)||1;kill(q,dx/d,dz/d,0,0,2)}
 if(boss&&boss.st=='a'&&Math.hypot(boss.x-D.mx,boss.z-D.mz)<48)hurt(boss,20,boss.x-D.mx,boss.z-D.mz)}
// zombie kena: blackhole mini muncul di badannya, jiwanya terbang ke tangan karakter terdekat
function duoSoul(q){const D=DUOA;if(!D||D.cl)return;let w=D.pl[0],bd=1e9;for(const x of D.pl){const d=Math.hypot(x.x-q.x,x.z-q.z);if(d<bd){bd=d;w=x}}
 const i=D.pl.indexOf(w),y=(q.y||0)+1.1;D.aim[i]={x:q.x,z:q.z,t:D.t};
 let m=MB.find(o=>!o.on);if(!m&&MB.length<8){m={b:mkBH(),on:0};MB.push(m)}
 if(m){Object.assign(m,{on:1,t:0,o:0,x:q.x,y,z:q.z,w});spr(q.x,y,q.z,2.6,0xffffff,.16,1.5,1)}
 const tp=tipPos(w);bolt(tp.x,tp.y,q.x,q.z,y,tp.z,i?0xbff6ff:0xcdb8ff)}
function updMini(dt){for(const m of MB){if(!m.on)continue;m.t+=dt;const t=m.t,r=t<.22?.85*ss(t/.22):t<.5?.85+Math.sin(t*30)*.04:.85*(1-ss(cl((t-.5)/.2,0,1)));setBH(m.b,m,r,dt);
 if(t>.28&&!m.o){m.o=1;spawnOrb(m.x,m.y,m.z,m.w)}
 if(t>=.7){m.on=0;setBH(m.b,m,0,dt)}}}
function spawnOrb(x,y,z,w){if(OR.length>=24)return;const s=spr(x,y,z,.8,w.h=='a'?0xd0b0ff:0xc8f4ff,1e9,0,1,1);if(!s)return;s.fixed=1;s.m.material.opacity=1;OR.push({s,x0:x,y0:y,z0:z,w,t:0,st:0})}
function relOrb(o){o.s.on=0;o.s.fixed=0;o.s.m.visible=false}
function updOrbs(D,dt){const c=D.c;for(const o of OR){o.t+=dt;const tp=tipPos(o.w);let x,y,z,k;
 if(o.st==0){k=ss(cl(o.t/.5,0,1));x=o.x0+(tp.x-o.x0)*k;z=o.z0+(tp.z-o.z0)*k;y=o.y0+(tp.y-o.y0)*k+Math.sin(k*PI)*1.2;if(o.t>=.5){o.st=1;o.t=0}}
 else{k=ss(cl(o.t/.45,0,1));x=tp.x+(c.x-tp.x)*k;z=tp.z+(c.z-tp.z)*k;y=tp.y+(c.y-tp.y)*k;if(o.t>=.45){D.g++;D.pu=1;o.dead=1}}
 o.s.m.position.set(x,y,z);o.s.m.scale.setScalar(.9+(o.st?.5*Math.sin(cl(o.t/.45,0,1)*PI):0));if(R()<.2)spr(x,y,z,.5,o.w.h=='a'?0xc0a0ff:0xbff6ff,.25,0,1,.6)}
 for(let j=OR.length-1;j>=0;j--)if(OR[j].dead){relOrb(OR[j]);OR.splice(j,1)}}
const DT=new T.Vector3(),DL=new T.Vector3(),DK=new T.Vector3(),DKF=[[0,8,1.9,0,1.4,60],[1,5,1.6,.12,1.6,52],[1.75,3.3,1.7,.2,1.75,44],[2.5,6.5,3.6,.55,3.4,58],[3.4,11,5.8,1.1,4.8,62],[6.2,14,6,2.2,6.5,66],[7.3,16,4,2.9,10.5,78],[8.1,15,3.2,3.3,3.5,68],[9.8,26,10,3.7,2,64]];
function duoCam(dt){const u=me.u,D=u.D;if(!D)return;const T=D.t-D.ta+1,mx=D.mx,mz=D.mz,th=-D.ph0;
 let i=0;while(i<DKF.length-2&&T>DKF[i+1][0])i++;const a0=DKF[i],a1=DKF[i+1],kk=ss(cl((T-a0[0])/(a1[0]-a0[0]),0,1)),v=a0.map((x,j)=>x+(a1[j]-x)*kk),a=th+v[3];
 DT.set(mx+Math.sin(a)*v[1],v[2],mz+Math.cos(a)*v[1]);DL.set(mx,v[4],mz);if(!u.cm){u.cm=1;DK.set(me.x,1.7,me.z)}
 cam.position.lerp(DT,1-Math.exp(-4.5*dt));DK.lerp(DL,1-Math.exp(-5*dt));cam.position.x+=(R()-.5)*shk;cam.lookAt(DK);cam.fov+=(v[5]-cam.fov)*(1-Math.exp(-4*dt));cam.updateProjectionMatrix()}

// ===== input
const keys={};let jx=0,jz=0,jo=null,co=null;
addEventListener('keydown',e=>{keys[e.code]=1;if(!me)return;if(e.code=='KeyJ')atk();if(e.code=='KeyK')skill(1);if(e.code=='KeyL')skill(2);if(e.code=='KeyU')skill(3)});addEventListener('keyup',e=>keys[e.code]=0);
addEventListener('touchstart',e=>{if(!me)return;for(const t of e.changedTouches){if(t.target.closest&&t.target.closest('button'))continue;if(t.clientX<innerWidth*.45){if(!jo){jo={id:t.identifier,x:t.clientX,y:t.clientY};const r=$('jr');r.style.display='block';r.style.left=jo.x+'px';r.style.top=jo.y+'px'}}else if(!co)co={id:t.identifier,x:t.clientX,y:t.clientY}}},{passive:true});
addEventListener('touchmove',e=>{for(const t of e.changedTouches){if(co&&t.identifier==co.id){yaw-=(t.clientX-co.x)*.008;pit=cl(pit+(t.clientY-co.y)*.005,.08,.9);co.x=t.clientX;co.y=t.clientY}if(jo&&t.identifier==jo.id){let dx=(t.clientX-jo.x)/50,dy=(t.clientY-jo.y)/50;const l=Math.hypot(dx,dy);if(l>1){dx/=l;dy/=l}jx=dx;jz=dy;$('jk').style.transform=`translate(${dx*30}px,${dy*30}px)`}}},{passive:true});
const te=e=>{for(const t of e.changedTouches){if(co&&t.identifier==co.id)co=null;if(jo&&t.identifier==jo.id){jo=null;jx=jz=0;$('jr').style.display='none';$('jk').style.transform=''}}};addEventListener('touchend',te);addEventListener('touchcancel',te);
let md=0;rd.domElement.addEventListener('mousedown',()=>md=1);addEventListener('mouseup',()=>md=0);addEventListener('mousemove',e=>{if(md&&me){yaw-=e.movementX*.006;pit=cl(pit+e.movementY*.004,.08,.9)}});
[['b0',atk],['b1',()=>skill(1)],['b2',()=>skill(2)],['b3',()=>skill(3)]].forEach(([i,f])=>$(i).addEventListener('pointerdown',e=>{e.preventDefault();f()}));

const upd=dt=>{for(const o of SP)if(o.on&&!o.fixed){o.l-=dt;const p=1-o.l/o.d;if(o.l<=0){o.on=0;o.m.visible=false;continue}o.m.scale.setScalar(o.s*(1+o.g*p));o.m.material.opacity=o.op*Math.pow(1-p,1.4);o.m.position.x+=o.v[0]*dt;o.m.position.y+=o.v[1]*dt;o.m.position.z+=o.v[2]*dt;o.v[0]*=.97;o.v[2]*=.97}
for(const o of RG)if(o.on){o.l-=dt;const p=1-o.l/o.d;if(o.l<=0){o.on=0;o.m.visible=false;continue}o.m.scale.setScalar(.3+o.r*ss(p));o.m.material.opacity=1-p}
for(let i=FX.length-1;i>=0;i--){const f=FX[i];f.l-=dt;f.fn(1-f.l/f.d);if(f.l<=0)FX.splice(i,1)}updCh(dt);updBt(dt,performance.now()/1000);invUp(dt)};

// ===== jaringan & lobi
const SKD={k:[['Serang','Tongkat sihir 3 hit, hit ke-3 lempar bom api'],['Kagebunshin','12 klon Klee menebas zombie (18 dtk)'],['Kupu-kupu','Zombie di sekitar berubah jadi kupu-kupu'],['ULT','Terbang, laser menyapu 360° hancurkan semua zombie']],
 l:[['Serang','Angin sepoi 3 hit ke zombie'],['Topan','3 tornado angkat & banting zombie'],['Dinding Angin','Zombie masuk terpental & hancur (15 dtk)'],['ULT','Terbang, petir + topan raksasa musnahkan semua zombie. Aether bisa ngajak kamu GERHANA KEMBAR (Terima/Tolak)']],
 a:[['Serang','Tebasan bayangan 3 hit ke zombie'],['Dash Bayangan','Melesat 11 m menembus & menebas zombie'],['Sambaran','6 petir menyambar berurutan ke depan'],['ULT','Singularitas: lubang hitam menyedot lalu meledak. Ajak Lumine (maks 25 m): kalau dia terima = GERHANA KEMBAR: tatap, tos, terbang, serang bareng, blackhole raksasa dihantam ke tanah!']]},HN={k:'Klee',l:'Lumine',a:'Aether'};
let LB={},mySlot=1,inGame=0;
['1','2'].forEach(n=>{$('s'+n).innerHTML=`<div class="hd">PEMAIN ${n}</div><div class="nm" id="n${n}">-</div><canvas id="pv${n}" width="150" height="180"></canvas><div class="hr" id="hr${n}"><button data-h="k">Klee</button><button data-h="l">Lumine</button><button data-h="a">Aether</button></div><div class="hn" id="hn${n}"></div><ul class="sk" id="sk${n}"></ul><button class="dn" id="dn${n}">Done</button>`;
 $('dn'+n).onclick=()=>{const o=LB[myid];if(o&&o.slot==n&&o.h)net({t:'done',d:o.d?0:1})}});
document.querySelectorAll('.hr button').forEach(b=>b.onclick=()=>{const o=LB[myid];if(o&&!o.d)net({t:'pick',h:b.dataset.h})});$('solo').onclick=()=>net({t:'solo'});
const pr=new T.WebGLRenderer({alpha:true,antialias:false}),pcam=new T.PerspectiveCamera(32,150/180,.1,30),PV={};pr.setPixelRatio(1);pr.setSize(150,180);pr.outputEncoding=T.sRGBEncoding;pcam.position.set(0,1.15,4.6);pcam.lookAt(0,.95,0);
function mkPV(k){const s=new T.Scene();s.add(new T.HemisphereLight(0xffffff,0x667788,1.1));const d=new T.DirectionalLight(0xffffff,.8);d.position.set(2,4,3);s.add(d);const r=wrig(k);s.add(r.root);return{s,e:{r,x:0,z:0,y:0,ry:0,spd:0,ph:0,fly:0,hy:0,an:null,h:k,rem:1,n:0,cd:1}}}
function pvLoop(dt){for(const n of[1,2]){const pl=Object.values(LB).find(o=>o.slot==n),c2=$('pv'+n).getContext('2d');c2.clearRect(0,0,150,180);if(!pl||!pl.h)continue;const v=PV[pl.h]||(PV[pl.h]=mkPV(pl.h)),e=v.e;e.ry=Math.sin(performance.now()/1000*.8)*.7+.2;e.cd-=dt;if(e.cd<=0&&!e.an){e.cd=2.6;e.an={k:'a'+(e.n++%3),t:0,d:.7,hit:9}}stepAn(e,dt);pose(e,dt);pr.render(v.s,pcam);c2.drawImage(pr.domElement,0,0)}}
function renderLb(){const all=Object.values(LB),mine=LB[myid];for(const n of[1,2]){const pl=all.find(o=>o.slot==n),isMe=!!pl&&pl.id==myid;$('s'+n).classList.toggle('me',isMe);$('n'+n).textContent=pl?pl.n+(isMe?' (kamu)':''):'Menunggu teman... kode: '+room;$('hr'+n).style.display=isMe?'flex':'none';
 if(isMe)[...$('hr'+n).children].forEach(b=>{const h=b.dataset.h;b.classList.toggle('on',pl.h==h);b.disabled=all.some(o=>o.id!=pl.id&&o.h==h)||!!pl.d});
 $('hn'+n).textContent=pl?(pl.h?HN[pl.h]:'Belum memilih karakter'):'';$('sk'+n).innerHTML=pl&&pl.h?SKD[pl.h].map(s=>'<li><b>'+s[0]+'</b>: '+s[1]+'</li>').join(''):'';
 const b=$('dn'+n);b.style.display=pl?'block':'none';if(isMe){b.disabled=!pl.h;b.textContent=pl.d?'Batal (sudah Done)':'Done';b.classList.toggle('on',!!pl.d)}else if(pl){b.disabled=true;b.textContent=pl.d?'SIAP ✓':'Memilih...';b.classList.toggle('on',!pl.d)}}
 $('solo').style.display=(mine&&mine.d&&all.length<2)?'block':'none'}
function spawnZ(){R=mul(4242);const pos=()=>{let x,z,t=0;do{x=rr(-95,95);z=rr(-95,95)}while(t++<30&&Math.hypot(x,z)<30);return[x,z]};
for(let i=0;i<99;i++){let[x,z]=pos(),hs=null;if(i<9){hs=H[i%H.length];x=hs.x+rr(-3,3);z=hs.z+rr(-3,3)+hs.rad+2}const q=mkZ(i<14?'r':'n',x,z);q.i=i;q.tx=x;q.tz=z;q.ty=0;if(hs){q.wr=1;q.hs=hs}Z.push(q)}R=Math.random}
function spawnBoss(x,z){boss=mkZ('b',x,z);boss.i=Z.length;boss.tx=x;boss.tz=z;boss.ty=0;Z.push(boss);msg('BOS GEMUK MUNCUL! 2 pukulannya membunuhmu')}
function syncTo(id){net({t:'y',d:Z.filter(q=>q.st=='d').map(q=>q.i),H:H.map(h=>h.hp|0),b:boss?[boss.x,boss.z]:0,c:crown?[crown.position.x,crown.position.z]:0},id)}
function startGame(m){const first=!inGame,fresh=[];if(first){inGame=1;$('lb').style.display='none';$('hud').style.display='block';pr.dispose();try{pr.forceContextLoss()}catch(e){}spawnZ()}
 for(const o of m.pl)if(!P[o.id]){addPlayer(o.id,o.n,o.h,o.id==myid);fresh.push(o.id)}me=P[myid];if(!me)return;$('b1').textContent=HM[me.h].n[0];$('b2').textContent=HM[me.h].n[1];if(first)msg('Kode tim: '+room);if(HOST&&!first)for(const id of fresh)if(id!=myid)syncTo(id)}
function onmsg(m){const p=P[m.f];switch(m.t){
case'w':myid=m.id;mySlot=m.slot;HOST=m.host;for(const o of m.pl)LB[o.id]=o;LB[myid]={id:myid,n:myn,h:null,d:0,slot:m.slot};$('m').style.display='none';$('lb').style.display='flex';$('lc').textContent=room;renderLb();break;
case'full':$('er').textContent='Tim penuh (maks 2 pemain)';ws.close();break;
case'j':LB[m.id]={id:m.id,n:m.n,h:null,d:0,slot:m.slot};if(!inGame)renderLb();break;
case'lb':{const o=LB[m.id];if(o){o.h=m.h;o.d=m.d}if(!inGame)renderLb();break}
case'go':startGame(m);break;
case'l':{delete LB[m.id];const q=P[m.id];if(q){sc.remove(q.r.root);delete P[m.id];msg('Teman keluar')}if(!inGame)renderLb();break}
case'host':HOST=m.id==myid;if(HOST&&inGame)msg('Kamu jadi host sekarang');break;
case'y':for(const i of m.d){const q=Z[i];if(q&&q.st!='d'){q.st='d';q.gone=1;sc.remove(q.r.root);kills++}}m.H.forEach((v,i)=>H[i].hp=v);if(m.b&&!boss)spawnBoss(m.b[0],m.b[1]);if(m.c&&!crown)mkCrown(m.c[0],m.c[1]);break;
case'p':if(p){p.tx=m.x;p.tz=m.z;p.ry=m.ry;p.spd=m.sp;p.thy=m.hy;p.tfy=m.fy;p.hp=m.hp;p.vx=m.vx;p.vz=m.vz}break;
case'a':if(p){p.tx=p.x=m.x;p.tz=p.z=m.z;p.ry=m.ry;doAtk(p,m.c)}break;
case's':if(p){p.tx=p.x=m.x;p.tz=p.z=m.z;p.ry=m.ry;DUOF=m.d?1:0;doSkill(p,m.n);DUOF=0}break;
case'z':if(!HOST)for(const a of m.a){const q=Z[a[0]];if(!q||q.st=='d')continue;q.tx=a[1];q.tz=a[2];q.ty=a[3];q.ry=a[4];q.spd=a[5];q.hp=a[7];q.st=a[6]?'l':'a'}break;
case'H':if(!HOST)m.h.forEach((v,i)=>H[i].hp=v);break;
case'k':if(!HOST)applyKill(Z[m.i],m.vx,m.vz,m.vy,m.sh,m.bf);break;
case'h':spr(m.x,1.3,m.z,1.1,0xc8ff90,.25,1,1);break;
case'za':{const q=Z[m.i];if(q&&q.st!='d'&&!HOST){q.ry=m.ry;q.an={k:'za',t:0,d:m.d,hit:9}}break}
case'dmg':dmgMe(m.d);break;
case'boss':if(!HOST&&!boss)spawnBoss(m.x,m.z);break;
case'crown':if(!HOST&&!crown)mkCrown(m.x,m.z);break;
case'win':win();break;
case'inv':if(me&&me.h=='l'&&me.hp>0&&!me.u&&p&&p.h=='a'&&!IVR){IVR={from:m.f,t:4.5};$('ivt').textContent='Aether ngajak GERHANA KEMBAR!';$('iv').style.display='flex'}else net({t:'dec'},m.f);break;
case'acc':if(INV&&INV.to==m.f&&me&&me.hp>0&&!me.u){INV=null;skillGo(3,1)}break;
case'dec':if(INV&&INV.to==m.f){INV=null;msg('Lumine nolak, ULT solo');skillGo(3,0)}break}}
function fs(){const d=document.documentElement,f=d.requestFullscreen||d.webkitRequestFullscreen;try{if(f&&!document.fullscreenElement&&!document.webkitFullscreenElement){const r=f.call(d);if(r&&r.then)r.then(()=>{if(screen.orientation&&screen.orientation.lock)screen.orientation.lock('landscape').catch(()=>{})}).catch(()=>{})}else if(screen.orientation&&screen.orientation.lock)screen.orientation.lock('landscape').catch(()=>{})}catch(e){}}
addEventListener('pointerdown',fs);
function enter(){fs();myn=($('nm').value||'Pemain').trim().slice(0,10);room=($('cd').value||Math.random().toString(36).slice(2,6)).trim().toUpperCase().slice(0,8);$('er').textContent='Menghubungkan...';
ws=new WebSocket((location.protocol=='https:'?'wss://':'ws://')+location.host);ws.onopen=()=>ws.send(JSON.stringify({t:'join',n:myn,c:room}));ws.onmessage=e=>onmsg(JSON.parse(e.data));ws.onclose=()=>{if(!won){msg('Koneksi putus. Muat ulang halaman');$('er').textContent='Koneksi putus'}};ws.onerror=()=>{$('er').textContent='Gagal terhubung ke server'}}
$('go').onclick=enter;
// ===== loop utama (1 loop = render 60fps)
const rc=$('rd').getContext('2d');$('rd').width=$('rd').height=92;let last=performance.now(),hudT=0,smT=0;
function loop(){requestAnimationFrame(loop);const now=performance.now(),dt=Math.min(.05,(now-last)/1000);last=now;const T0=now/1000;
CL.forEach((c,i)=>{c.position.x+=dt*(1+i%3)*.6;if(c.position.x>170)c.position.x=-170});upd(dt);if(!inGame)pvLoop(dt);
if(me){
 for(const id in P){const p=P[id],L=p==me;
  if(L){if(!p.u&&p.hp>0){let ix=jx+((keys.KeyD?1:0)-(keys.KeyA?1:0)),iz=jz+((keys.KeyS?1:0)-(keys.KeyW?1:0));const l=Math.hypot(ix,iz);if(l>1){ix/=l;iz/=l}const cs=Math.cos(yaw),sn=Math.sin(yaw),ax=cs*ix+sn*iz,az=-sn*ix+cs*iz,spd=p.an?l*2.2:l*6.2;p.x+=ax*spd*dt;p.z+=az*spd*dt;p.vx=ax*spd;p.vz=az*spd;p.spd+=(spd-p.spd)*Math.min(1,dt*10);if(l>.1&&!p.an)p.ry=Math.atan2(ax,az);pushOut(p,.5)}else{p.vx=p.vz=0;p.spd*=.8}
   if(p.hp<=0&&!over0(p)){p.down-=dt;if(p.down<=0){p.hp=60;p.inv=3;msg('Bangkit!')}}for(let i=1;i<4;i++)p.cd[i]=Math.max(0,p.cd[i]-dt);if(p.hp>0&&p.hp<100&&!p.u)p.hp=Math.min(100,p.hp+dt*.8)}
  else{p.tx+=p.vx*dt;p.tz+=p.vz*dt;const k=Math.min(1,dt*14);p.x+=(p.tx-p.x)*k;p.z+=(p.tz-p.z)*k}
  stepAn(p,dt);if(p.u)ultUp(p,dt);else if(L){p.hy+=(0-p.hy)*Math.min(1,dt*5);p.fly+=(0-p.fly)*Math.min(1,dt*5)}else{const k=Math.min(1,dt*10);p.hy+=(p.thy-p.hy)*k;p.fly+=(p.tfy-p.fly)*k}
  p.cbt-=dt;p.inv-=dt;pose(p,dt);trail(p);const r=p.r.root;r.rotation.x+=((p.hp<=0?-1.5:0)-r.rotation.x)*Math.min(1,dt*8);if(p.hp<=0)r.position.y=.15}
 for(const c of C){c.life-=dt;let b=null,bd=45;for(const q of Z)if(q.st=='a'){const d=Math.hypot(q.x-c.x,q.z-c.z);if(d<bd){bd=d;b=q}}let sp=0;
  if(b){c.ry=Math.atan2(b.x-c.x,b.z-c.z);if(bd>1.5)sp=9;else if(!c.an){c.t0++;c.an={k:'a'+(c.t0%2),t:0,d:.45,hit:.45,cb:()=>{if(b.st=='a'){spr(b.x,1.2,b.z,1.4,0xffffff,.2,1,1);hurt(b,22,b.x-c.x,b.z-c.z)}}}}}
  else{const dx=c.o.x-c.x,dz=c.o.z-c.z,d=Math.hypot(dx,dz);if(d>6){c.ry=Math.atan2(dx,dz);sp=7}}
  if(sp){c.x+=Math.sin(c.ry)*sp*dt;c.z+=Math.cos(c.ry)*sp*dt}c.spd+=(sp-c.spd)*Math.min(1,dt*8);stepAn(c,dt);pose(c,dt);trail(c);if(c.life<=0){puff(c.x,c.z);sc.remove(c.r.root);c.dead=1}}
 C=C.filter(c=>!c.dead);
 for(const q of Z){if(q.gone)continue;const dm=Math.hypot(q.x-me.x,q.z-me.z);
  if(q.st=='d'){zdead(q,dt);continue}
  if(HOST){if(q.st=='l')continue;q.r.root.visible=dm<80;zup(q,dt);stepAn(q,dt);if(q.r.root.visible)pose(q,dt)}
  else{const k=Math.min(1,dt*16);q.x+=(q.tx-q.x)*k;q.z+=(q.tz-q.z)*k;q.y+=(q.ty-q.y)*k;q.r.root.visible=dm<80||q.st=='l';
   if(q.st=='l'){q.lt=(q.lt||0)+dt;const r=q.r.root;r.rotation.y+=dt*9;r.rotation.z=Math.sin(q.lt*5)*.6;r.position.set(q.x,q.y,q.z);for(const j of[...q.r.sh,...q.r.hp])j.rotation.x+=(Math.sin(q.lt*10)*2-j.rotation.x)*.2}
   else if(q.r.root.visible){stepAn(q,dt);pose(q,dt)}}}
 for(const id in P){const p=P[id],w=p.wall;if(!w)continue;w.l-=dt;w.m.position.set(p.x,2.2,p.z);w.t.offset.x+=dt*.8;w.m.rotation.y+=dt*2;w.m.material.opacity=Math.min(1,w.l);
  if(HOST)for(const q of Z){if(q.st!='a')continue;const dx=q.x-p.x,dz=q.z-p.z,d=Math.hypot(dx,dz);if(!q.bs&&d<6){kill(q,dx/d*2.5,dz/d*2.5,9,1);spr(q.x,1.5,q.z,3,0xbff6ff,.4,1.5,1)}else if(q.bs&&d<6.5){hurt(q,80,dx,dz);q.cd=1.5}}
  if(w.l<=0){sc.remove(w.m);p.wall=null}}
 for(const w of TW){w.l-=dt;w.x+=w.dx*8*dt;w.z+=w.dz*8*dt;w.m.position.set(w.x,0,w.z);w.m.children.forEach(m=>m.rotation.y+=dt*m.userData.sp*10);const gs=cl(w.l,0,1);w.m.scale.set(gs,1,gs);
  if(HOST){for(const q of Z)if(q.st=='a'&&!q.bs&&Math.hypot(q.x-w.x,q.z-w.z)<5.5){q.st='l';q.la=R()*6;q.lr=Math.hypot(q.x-w.x,q.z-w.z);q.lh=0;q.lt=0;w.lift.push(q)}
  for(const q of w.lift)if(q.st=='l'){q.lt+=dt;q.la+=dt*(5+q.lt*2);q.lr+=(1.6-q.lr)*dt*2;q.lh=Math.min(7,q.lh+dt*4);q.x=w.x+Math.cos(q.la)*q.lr;q.z=w.z+Math.sin(q.la)*q.lr;q.y=q.lh;q.r.root.rotation.y+=dt*9;q.r.root.rotation.z=Math.sin(q.lt*5)*.6;q.r.root.position.set(q.x,q.y,q.z);for(const j of[...q.r.sh,...q.r.hp])j.rotation.x+=(Math.sin(q.lt*10)*2-j.rotation.x)*.2;
   if(q.lt>2.2||w.l<=0){q.st='a';q.hp=0;kill(q,Math.cos(q.la+1.57)*2,Math.sin(q.la+1.57)*2,4,0);q.y=q.lh}}}}
 TW=TW.filter(w=>{if(w.l<=0){sc.remove(w.m);return false}return true});
 for(const w of WV){w.l-=dt;w.x+=w.dx*16*dt;w.z+=w.dz*16*dt;w.m.position.set(w.x,w.y,w.z);w.m.material.opacity=cl(w.l*2,0,1);if(R()<.6)spr(w.x+rr(-1,1),rr(.5,1.8),w.z+rr(-1,1),.8,0xe8ffff,.5,1,1,.7);if(HOST)for(const q of Z)if(q.st=='a'&&!w.hit.has(q)&&Math.hypot(q.x-w.x,q.z-w.z)<w.sz+.8){w.hit.add(q);hurt(q,w.dmg,w.dx,w.dz)}if(w.l<=0)sc.remove(w.m)}WV=WV.filter(w=>w.l>0);
 for(const h of H){if(h.hp<100&&h.hp>0){const u=1-h.hp/100;h.m.rotation.z=u*.12*Math.sin(h.ry);h.m.position.y=-u*.5;if(R()<dt*2)spr(h.x+rr(-3,3),rr(2,4),h.z+rr(-3,3),2.4,0x2a2622,2,1.5,0,.5,0,2,0)}
  else if(h.hp<=0&&!h.col){h.col=1;h.m.visible=false;h.rub.visible=true;boom(h.x,1.5,h.z,3,0xb09a80);for(let i=0;i<4;i++)spr(h.x+rr(-3,3),2,h.z+rr(-3,3),6,0x8a7a6a,2.5,1.5,0,.7,0,1.5,0)}}
 if(crown){crown.rotation.y+=dt*2;crown.position.y=1+Math.sin(T0*3)*.2;if(!won&&me.hp>0&&Math.hypot(crown.position.x-me.x,crown.position.z-me.z)<2.5){sc.remove(crown);crown=null;net({t:'win'});win()}}
 const al=alive().length;
 if(HOST){if(!boss&&al<=25){const x=cl(me.x+30,-95,95),z=cl(me.z+30,-95,95);spawnBoss(x,z);net({t:'boss',x,z})}
  snapT-=dt;if(snapT<=0){snapT=.05;slowT-=.05;const full=slowT<=0;if(full)slowT=1;const a=[],pl=Object.values(P);for(const q of Z){if(q.st=='d'||q.gone)continue;if(!full&&!pl.some(p=>Math.hypot(p.x-q.x,p.z-q.z)<90))continue;a.push([q.i,r2(q.x),r2(q.z),r2(q.y),r2(q.ry),Math.round(q.spd*10)/10,q.st=='l'?1:0,q.hp|0])}if(a.length)net({t:'z',a});if(full)net({t:'H',h:H.map(h=>h.hp|0)})}}
 sendT-=dt;if(sendT<=0){sendT=.033;net({t:'p',x:r2(me.x),z:r2(me.z),ry:r2(me.ry),sp:r2(me.spd),hy:r2(me.hy),fy:r2(me.fly),hp:me.hp|0,vx:r2(me.vx||0),vz:r2(me.vz||0)})}
 if(me.u&&me.u.duo)duoCam(dt);else if(!me.u){if(cam.fov!=60){cam.fov+=(60-cam.fov)*.15;if(Math.abs(cam.fov-60)<.2)cam.fov=60;cam.updateProjectionMatrix()}const cs=Math.sin(yaw),cc=Math.cos(yaw),cp=Math.cos(pit),sp=Math.sin(pit),D=8,tx=me.x+cc*1.1,tz=me.z-cs*1.1;cam.position.lerp(new T.Vector3(tx+cs*D*cp,1.8+D*sp,tz+cc*D*cp),.18);cam.position.x+=(R()-.5)*shk;cam.lookAt(tx,1.7,tz)}
 else{const u=me.u,t=u.t,a=u.a+PI+t*.7,Rd=t<1.3?6+t*3:12+Math.min(t,4)*1.5,cy=t<1.3?.8+t*2:me.hy+1+Math.sin(t)*2.5;cam.position.lerp(new T.Vector3(me.x+Math.sin(a)*Rd,cy,me.z+Math.cos(a)*Rd),.1);cam.lookAt(me.x,me.hy+1,me.z)}
 shk*=.9;fl.intensity*=.9;sky.position.copy(cam.position);
 smT-=dt;if(smT<=0){smT=.4;for(const f of FS)if(Math.hypot(f[2][0]-me.x,f[2][1]-me.z)<50)spr(f[2][0],2,f[2][1],3,0x2a2a2a,3.5,2,0,.5,.3,2.2,0)}
 FS.forEach((f,i)=>{f[0].m.scale.set(3.2+Math.sin(T0*13+i)*.6,3.8+Math.sin(T0*17+i*2)*.9,1);f[1].m.scale.set(1.8+Math.sin(T0*19+i)*.4,2.4+Math.sin(T0*11+i)*.6,1)});
 hudT-=dt;if(hudT<=0){hudT=.12;$('zc').textContent='Tim '+room+' · Zombie '+al+'/100 · '+Object.keys(P).length+' pemain';$('hp').style.width=me.hp+'%';[1,2,3].forEach(i=>{const b=$('b'+i),c=me.cd[i];b.style.opacity=c>0?.45:1;b.textContent=c>0?Math.ceil(c):(i<3?HM[me.h].n[i-1]:'ULT')});
  rc.clearRect(0,0,92,92);rc.fillStyle='#ff4040';for(const q of Z)if(q.st!='d'){const dx=(q.x-me.x)/60*44,dz=(q.z-me.z)/60*44,x=Math.cos(yaw)*dx-Math.sin(yaw)*dz,y=Math.sin(yaw)*dx+Math.cos(yaw)*dz;if(Math.hypot(x,y)<44)rc.fillRect(46+x-1.5,46+y-1.5,q.bs?5:3,q.bs?5:3)}
  rc.fillStyle='#6ff';for(const id in P){const p=P[id];if(p==me){rc.fillRect(44,44,4,4);continue}const dx=(p.x-me.x)/60*44,dz=(p.z-me.z)/60*44;rc.fillStyle='#6f6';rc.fillRect(46+cl(Math.cos(yaw)*dx-Math.sin(yaw)*dz,-42,42),46+cl(Math.sin(yaw)*dx+Math.cos(yaw)*dz,-42,42),4,4)}
  if(crown){const dx=(crown.position.x-me.x)/60*44,dz=(crown.position.z-me.z)/60*44;rc.fillStyle='#fd3';rc.fillRect(46+cl(Math.cos(yaw)*dx-Math.sin(yaw)*dz,-42,42),46+cl(Math.sin(yaw)*dx+Math.cos(yaw)*dz,-42,42),5,5)}}
 if(msgT>0&&(msgT-=dt)<=0)$('ms').style.opacity=0}
if(inGame)rd.render(sc,cam)}
function over0(){return false}
loop();

</script></body></html>
HTML>>*/
