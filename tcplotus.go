// Copyright 2019 BILL. All rights reserved.
// Use of this source code is governed by a GPL-style
// license that can be found in the LICENSE file.

/*
	Package flag implements command-line flag parsing.

	Usage, see:
        tcplotus -h 

*/

package main

import (
	"encoding/binary"
	"encoding/hex"

	//"encoding/json"
	"bytes"
	"errors"
	"flag"
	"log"
	"net"
	_ "os"
	_ "reflect"
	_ "runtime"
	"strconv"
	"time"
	"strings"
	"sync"
)

const (
	PXY_MSG_BUF_COUNT = 1 * 1000 // 对上的消息缓冲大小(消息个数)
	//msg_length        = 4 * 1024
	chan_buf_len      = 1024 * 1024 //
	QP_FLG_REQ        = "Q"

	QP_FLG_RESP      = "P"
	LEN_TPDU         = 4
	TPDU_OFFSET_POSI = 3
	MSG_LEN_LEN      = 2
	MAX_MSG_LEN      = 4 * 1024
	SYS_CPU_NUM      = 4
	MAX_REQ_ID       = 1024
	IDLE_PKG_INTERVAL = 55
	RQ_MAP_SLICE_SIZE = 32
)

type Request struct {
	reqId           int
	reqOriTpdu      []byte
	reqTpdu         []byte
	reqContent      []byte
	rspChan         chan<- []byte // writeonly chan
	left_in_count   int64
	left_out_count  int64
	right_in_count  int64
	right_out_count int64
	left_in_tm      int64   // ns
	left_out_tm     int64   // ns
	right_in_tm     int64   // ns	
	right_out_tm    int64   // ns		
}

//store request map
// var requestMap map[int]*Request

type Clienter struct {
	c_id	int
	sAddr   string
	client  net.Conn
	isAlive bool
	SendStr chan *Request
	RecvStr chan []byte
	requestMap map[int]*Request
}

var IDLE_MSG []byte  = make([]byte, MSG_LEN_LEN)

func (c *Clienter) Connect() bool {
	if c.isAlive {
		return true
	} else {
		var err error
		c.client, err = net.Dial("tcp", c.sAddr)
		if err != nil {
			log.Printf("ERROR : Dail failed!, errinfo:%s\n",err.Error())
			return false
		}
		c.isAlive = true
		log.Println("connect to " + c.sAddr)
	}
	return true
}

//send msg to upstream server
func ProxySendLoop(c *Clienter) {

	//store reqId and reqContent
	//senddata := make(map[string]string)
	//var right_out_count int64
	for {
		if !c.isAlive {
			time.Sleep(1 * time.Second)
			c.Connect()
		}
		if c.isAlive {
			select {

				case req := <-c.SendStr  :

					//log.Printf("ProxySendLoop read data from SendStr, reqId:%d\n", req.reqId)

					//_, err = c.client.Write([]byte(sendjson))
					// len := len(req.reqContent)

					// log.Printf("RIGHT OUT with id[%d], len:%d, data:% x\n",req.reqId, len, req.reqContent[0:len])
					req.right_out_count++

					reqTpdu_src := req.reqContent[TPDU_OFFSET_POSI+(LEN_TPDU/2) : TPDU_OFFSET_POSI+LEN_TPDU]

					reqTpdu_dst := req.reqContent[TPDU_OFFSET_POSI : TPDU_OFFSET_POSI+(LEN_TPDU/2)]

					// 将序号拷入tpdu.dst 字段
					copy(reqTpdu_dst, Int64To2Bytes(req.right_out_count%10000))

					
					_, err := c.client.Write(req.reqContent)
					if err != nil {
						//c.RecvStr <- string("proxy server close...")
						c.client.Close()
						c.isAlive = false
						log.Println("RIGHT OUT write failed ! disconnect from " + c.sAddr + ":"+err.Error())
						continue
					}
					req.right_out_tm = time.Now().UnixNano()				
					log.Printf("id[%d.%d]RIGHT OUT:%d, reqTpdu_src:% x, reqTpdu_dst:% x, tm_used(us):%v\n", c.c_id, req.reqId, req.right_out_count, reqTpdu_src, reqTpdu_dst , (req.right_out_tm - req.left_in_tm)/1000)
	

					//log.Println("Write to proxy server: " + string(sendjson))
					//
				
				/*
				case <-time.After(IDLE_PKG_INTERVAL * time.Second):
					_, err := c.client.Write(IDLE_MSG)
					if err != nil {
						c.client.Close()
						c.isAlive = false
						log.Println("RIGHT OUT write idle pkg failed, disconnect from " + s_addr)
						continue						
					}
				*/
				}
		}
	}
}

func read_one_msg(conn net.Conn, msg_buf []byte) (len int, err error) {
	//data := make([]byte, MAX_MSG_LEN)

	if cap(msg_buf) < MAX_MSG_LEN {
		return -1, errors.New("msg_buf cap is not enouch !")
	}

	msg_len_data := msg_buf[0:MSG_LEN_LEN]

	// 读取len_len
	var received_len_len int 
	received_len_len = 0
	for received_len_len < MSG_LEN_LEN {
		n, err := conn.Read(msg_len_data[received_len_len:])
		if err != nil {
			log.Printf("Read fail: %s\n", err.Error())
			return -1, errors.New("Read data_len failed!")
		}
		received_len_len += n
		//log.Printf("n:%d, received_len_len:%d\n",n, received_len_len)
	}

	// 前两个字节转为整型长度
	msg_len := Bytes2ToInt(msg_len_data)
	//log.Printf("Received len:% x, msg_len:%d\n",msg_len_data,msg_len)

 	if (msg_len == 0 && bytes.Equal(msg_len_data, IDLE_MSG)){
 		return received_len_len, nil
 	}

	if msg_len+MSG_LEN_LEN > MAX_MSG_LEN {
		return -1, errors.New("received msg_len_len errors!")
	}

	// 读取data
	msg_data := msg_buf[MSG_LEN_LEN : MSG_LEN_LEN+msg_len]
	received_data_len := 0
	for received_data_len < msg_len {
		n, err := conn.Read(msg_data[received_data_len:])
		if err != nil {
			return -1, errors.New("Read data content failed!")
		}
		received_data_len += n
	}

	return received_len_len + received_data_len, nil
}




//recv msg from upstream server
func ProxyRecvLoop(c *Clienter) {
	buf := make([]byte, MAX_MSG_LEN)

	var right_in_count int64

	requestMap := c.requestMap 

	for {
		if !c.isAlive {
			time.Sleep(1 * time.Second)
			c.Connect()
		}
		if c.isAlive {
			n, err := read_one_msg(c.client, buf)
			//log.Printf("len(buf):%d\n",len(buf))
			if err != nil {
				log.Println(err)
				log.Printf("disconnect from server:%s \n", c.sAddr)
				c.client.Close()
				c.isAlive = false
				continue
			}

			if n < MSG_LEN_LEN {
				log.Printf("ERROR: invalid length, n:%d\n", n)
				continue
			}

			if (n ==MSG_LEN_LEN  && bytes.Equal(buf[0:MSG_LEN_LEN],IDLE_MSG)) {
				// log.Printf("RIGHT IN idle package received!");
				continue
			}


			// 根据 TPDU中dest两位来确定req_id
			//log.Printf("------------resp from server ---------\n")

			right_in_count++
			respTpdu_dst := buf[TPDU_OFFSET_POSI:(TPDU_OFFSET_POSI + (LEN_TPDU / 2))]

			//respTpdu_src := buf[TPDU_OFFSET_POSI+(LEN_TPDU/2) : TPDU_OFFSET_POSI+LEN_TPDU]			
			//log.Printf("id[ ]RIGHT IN:%d, respTpdu_dst:% x, respTpdu_src:% x\n", right_in_count, respTpdu_dst, respTpdu_src)

			////log.Printf("rsp Proxy From Server:: len:%d, data:%s ", one_msg_len, hex.EncodeToString(buf[0:one_msg_len]))

			// TODO: 应答 回来的数据中， tpdu会被前后对调，这里临时这样处理，后续需要改进
			if reqid, err := getReqIdByTpdu(respTpdu_dst, 0, QP_FLG_RESP); err == nil {
				req, ok := requestMap[reqid]
				if !ok {
					log.Printf("ERROR:c_id:%d GET invalid reqid:%d , respTpdu_dst:% x\n", c.c_id ,reqid, respTpdu_dst)
					continue
				}

				/* 根据接收到的信息中的TPDU，找到对应的记录，并判断是否跟原始请求的一致，如果不一致，可能是服务端未原样返回，直接丢弃 */

				reqTpdu_src := req.reqTpdu[(LEN_TPDU / 2):LEN_TPDU]

				//log.Printf("curTpdu:%s, req.reqTpdu:%s, snd to Client[%02d]\n", hex.EncodeToString(respTpdu_dst), hex.EncodeToString(reqTpdu_src), reqid)
				// if !reflect.DeepEqual(respTpdu_dst, reqTpdu_src) {
				
				if !bytes.Equal(respTpdu_dst, reqTpdu_src) {
					log.Printf("ERROR:curTpdu:%s not equal req.reqTpdu:%s\n", hex.EncodeToString(respTpdu_dst), hex.EncodeToString(reqTpdu_src))
					continue
				}

				mutex.Lock()
				req.right_in_tm = time.Now().UnixNano()
				req.right_in_count++
				mutex.Unlock()
				//log.Printf("id[%d]RIGHT IN:%d, respTpdu_dst:% x, respTpdu_src:% x\n", reqid, req.right_in_count, respTpdu_dst, respTpdu_src)
				/* 将应答放入应答通道中 */
				req.rspChan <- buf[0:n]
			}
		}
	}
}

func getReqIdByTpdu(tpdu []byte, id int, qp_flg string) (real_id int, err error) {
	tmp_len_buf := make([]byte, 4)
	copy(tmp_len_buf[2:4], tpdu[0:2])
	real_id = int(BytesToInt(tmp_len_buf))
	return real_id, nil
}

func getReqId(buf []byte, id int, qp_flg string) (real_id int, err error) {
	//var id int

	if qp_flg == QP_FLG_REQ {
		real_id = id
	} else {
		tpdu := buf[TPDU_OFFSET_POSI : TPDU_OFFSET_POSI+2]

		tmp_len_buf := make([]byte, 4)
		copy(tmp_len_buf[2:4], tpdu[0:2])

		real_id = int(BytesToInt(tmp_len_buf))

		//real_id = int(BytesToInt32(tpdu))
		//log.Printf("tmp_buf:%s\n", hex.EncodeToString(buf[3:7]))
	}

	//log.Printf("qp_flg:[%s]real_id:%d\n", qp_flg, real_id)
	return real_id, nil
}

func Int64To2Bytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf[6:8]
}

func Int32ToBytes(i int32) []byte {
	var buf = make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf
}

func BytesToInt32(buf []byte) int32 {
	return int32(binary.BigEndian.Uint32(buf))
}

//字节转换成整形
func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp int32
	binary.Read(bytesBuffer, binary.BigEndian, &tmp)
	return int(tmp)
}

func Bytes2ToInt(b []byte) int {
	tmp_v := make([]byte, 4)
	copy(tmp_v[2:4], b[0:2])

	bytesBuffer := bytes.NewBuffer(tmp_v)
	var tmp int32
	binary.Read(bytesBuffer, binary.BigEndian, &tmp)
	return int(tmp)
}

//one handle per request
func handle(conn *net.TCPConn, id int, prxSndChnl chan *Request , requestMap map[int]*Request, rq_map_idx int) {

	//requestMap := tc.requestMap 

	if nil == requestMap {
		log.Printf("ERROR: id[%d], tc.requestMap is ni\n",id);
		return 
	}

	defer func() {
		if _, ok := requestMap[id]; ok {
			log.Printf("delete from requestMap id:[%d]\n", id)
			delete(requestMap, id)
			conn.Close()
		}
	}()

	var real_req_id int
	real_req_id = id
	data := make([]byte, MAX_MSG_LEN)
	handleProxy := make(chan []byte, chan_buf_len)
	// request := &Request{reqId: id, rspChan: handleProxy}
	// requestMap[id] = request
	var request *Request
	var hd_count int64 = 1
	var left_out_count int64
	var left_in_count int64

	//var ori_tpdu [LEN_TPDU]byte
	ori_tpdu := make([]byte, LEN_TPDU)
	prx_tpdu := make([]byte, LEN_TPDU)

	var left_in_tm int64
	var  avg_tm_used int64 

	for {

		// 阻塞读
		n, err := read_one_msg(conn, data)
		if err != nil {
			log.Println(err)
			log.Printf("ERROR: left in read error, disconnect from Client:%s \n", conn.RemoteAddr().String())
			conn.Close()
			return
		}

		if n < 2 {
			continue
		}

		if (n ==MSG_LEN_LEN  && bytes.Equal(data[0:MSG_LEN_LEN],IDLE_MSG)) {
				// log.Printf("LEFT IN idle package received! ++ IDLE_MSG:% x, data:% x\n", IDLE_MSG, data[0:MSG_LEN_LEN])
				continue
		}

		// log.Printf(">>>>>>>>>>>>>>>>>msg [%d] handle begin>>>>>>>>>>>>>\n",hd_count)

		if log_lvl >= 3 {
			log.Printf("req data:% x\n", data[0:n])
		}

		/* 取报文头中的TPDU 后4个字节*/
		tpdu := data[TPDU_OFFSET_POSI : TPDU_OFFSET_POSI+LEN_TPDU]
		left_in_count++
		//log.Printf("id[%d]LEFT IN :%d ,tpdu:% x", id, left_in_count, tpdu)

		/* real_req_id 设置到tpdu的末4字节中 */
		// 数字转 []byte, 网络字节序为大端字节序
		//log.Printf("ori tpdu:%s", hex.EncodeToString(tpdu[0:4]))

		tmp_byte_buf := Int32ToBytes(int32(real_req_id))
		//log.Printf("tmp_byte_buf:%s", hex.EncodeToString(tmp_byte_buf))

		copy(ori_tpdu, tpdu)

		//替换原始请求报文中的tpdu 为新值
		copy(tpdu, tmp_byte_buf[0:LEN_TPDU])

		//log.Printf("after chg tpdu:%s", hex.EncodeToString(tpdu[0:4]))

		/* 如果不存在此id ，则新建，新放入requestMap中*/
		if _, ok := requestMap[real_req_id]; !ok {
			request = &Request{reqId: real_req_id, rspChan: handleProxy, reqContent: data[0:n]}
			request.reqOriTpdu = make([]byte, LEN_TPDU)
			request.reqTpdu = make([]byte, LEN_TPDU)
			requestMap[real_req_id] = request

			//request.reqContent = (data[0:n])
			//
			//log.Printf("new request:%d\n",real_req_id)
		} else {
			//log.Printf("old request:%d\n",real_req_id)
		}

		/* 将原始请求TPDU保存起来 */
		copy(request.reqOriTpdu, ori_tpdu)
		/* 将替换后的TPDU保存起来，用于后端异步应答时校验 */
		copy(request.reqTpdu, tpdu)

		request.left_in_tm = time.Now().UnixNano()
		left_in_tm = request.left_in_tm

		//log.Printf("Client[%02d] req To   Proxy::len:%d,request.reqOriTpdu:%s, reqTpdu:%s\n", id, n, hex.EncodeToString(request.reqOriTpdu), hex.EncodeToString(request.reqTpdu))
		//send to proxy
		select {

		case prxSndChnl <- request:
		case <-time.After(proxy_timeout * time.Second):
			//proxyChan <- &Request{cancel: true, reqId: id}
			//_, err = conn.Write([]byte("proxy server send timeout."))
			//if err != nil {
			conn.Close()
			delete(requestMap, real_req_id)
			return
			//}
			//continue
		}

		//read from proxy
		select {
		case rspContent := <-handleProxy:
			left_out_count++
			//log.Printf("Client[%02d] rsp From Proxy::len:%d, data:%s\n", id, len(rspContent), hex.EncodeToString(rspContent))

			//替换应答报文中的tpdu 为原始值
			rsp_tpdu := rspContent[TPDU_OFFSET_POSI : TPDU_OFFSET_POSI+LEN_TPDU]

			copy(prx_tpdu, rsp_tpdu)
			copy(rsp_tpdu, ori_tpdu)

			_, err := conn.Write([]byte(rspContent))
			if err != nil {
				log.Printf("ERROR: left out write failed!, prx_tpdu:% x\n", prx_tpdu)
				conn.Close()
				delete(requestMap, real_req_id)
				return
			}

			left_out_tm := time.Now().UnixNano()

			avg_tm_used +=  (left_out_tm-left_in_tm)/1000 
			//log.Printf("LEFT OUT with id[%d], len:%d, data:% x\n",id,len(rspContent), rspContent)
			//log.Printf("id[%d]LEFT OUT :%d prx_tpdu:% x, tm_used(us):%v \n", id, left_out_count, prx_tpdu, (right_out_tm-requestMap[real_req_id].right_in_tm)/1000)
			log.Printf("id[%d.%d]LEFT OUT :%d prx_tpdu:% x, l_in->l_out tm_used(us):%v \n", rq_map_idx, id, left_out_count, prx_tpdu, (left_out_tm-left_in_tm)/1000)

			if log_lvl >= 3 {
				log.Printf("resq data:% x\n", rspContent)
			}

			if hd_count % 1000 == 0 {
				log.Printf("id[%d.%d] average l_in->l_out tm_used(us):%d\n", rq_map_idx, id, avg_tm_used / hd_count);
				avg_tm_used = 0
			}

			//log.Printf("<<<<<<<<<<<<<<<<<<msg [%d] handle end<<<<<<<<<<<<<<<<<\n", hd_count)
			hd_count += 1
		case <-time.After(proxy_timeout * time.Second):
			//_, err = conn.Write([]byte("proxy server recv timeout."))
			//if err != nil {
			conn.Close()
				log.Printf("ERROR: left out time out, id:[%d]\n",real_req_id)			
			delete(requestMap, real_req_id)
			return
			//	}
			//continue
		}
	}
}

// req_id是否可用
func isIdAvail(requestMap map[int]*Request, req_id int) bool {
	_, ok := requestMap[req_id]
	return !ok
}


func bootTcpClient(tc *Clienter, c_id int, prxSndChnl chan *Request , requestMap map[int]*Request , s_addr string) {

		//start proxy connect and loop
	//var tc Clienter

	// Proxy向上发送的通道缓冲区大小, 所有后端发送的消息取自此通道
	//tc.SendStr = make(chan *Request, PXY_MSG_BUF_COUNT)

	tc.c_id = c_id 
	tc.SendStr = prxSndChnl
	tc.requestMap = requestMap
	tc.sAddr = s_addr
	tc.Connect()
	
	go ProxySendLoop(tc)
	go ProxyRecvLoop(tc)
}

func TcpLotusMain(ip string, port int, s_addr_list []string) {

	//var requestMap map[int]*Request
	//var prxSndChnl  chan []byte


	//requestMap := make(map[int]*Request)
	rq_map_slice := make([]map[int]*Request, RQ_MAP_SLICE_SIZE)
	prx_snd_chnl_slice := make([]chan *Request, PXY_MSG_BUF_COUNT)
	
	//prxSndChnl := make(chan *Request, PXY_MSG_BUF_COUNT)

	//log.Printf("lotusId[%d]:: local ip:%s, port:%d;  remote_addr:%s\n",lotusId, ip, port, s_addr)

	pxy_num := len(s_addr_list)
	var tc []Clienter = make ([]Clienter, pxy_num)

	for i:=0; i< pxy_num; i++{
	    log.Printf("start RIGHT tcp Client, num[%d]: connect to SERVER:[%s]\n",i, s_addr_list[i])		
	    rq_map_slice[i] = make(map[int]*Request)	    
	    prx_snd_chnl_slice[i] = make(chan *Request, PXY_MSG_BUF_COUNT)

	    // 每个后端通道对应一个 rq_map_slice[i], chanel
		bootTcpClient(&tc[i], i, prx_snd_chnl_slice[i], rq_map_slice[i], s_addr_list[i])
		time.Sleep(1 * time.Second)
	}

	//start tcp server

	listen, err := net.ListenTCP("tcp", &net.TCPAddr{net.ParseIP(ip), port, ""})
	if err != nil {
		log.Fatalln("listen port error")
		return
	}
	log.Println("start LEFT tcp server " + ip + " " + strconv.Itoa(port))
	defer listen.Close()

	// 前端作为服务端处理：
	//listen new request
	//requestMap = make(map[int]*Request)
	var id int
	var try_id int
	var rq_map_idx int 

	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			log.Println("receive connection failed")
			continue
		}

		mutex.Lock()
		var fnd_count int
		for fnd_count = 0; fnd_count < MAX_REQ_ID; fnd_count++ {
			id++
			try_id = id % MAX_REQ_ID


			/* 根据id划分至不同的处理通道对应的map slice*/
			rq_map_idx = id % pxy_num

			if isIdAvail(rq_map_slice[rq_map_idx], try_id) {
				log.Printf("id %d is avail\n", try_id)
				break
			} else {
				log.Printf("id %d is NOT avail\n", try_id)
			}
		}

		mutex.Unlock()

		if fnd_count == MAX_REQ_ID {
			log.Printf("Error: no req_id avail! \n")
			continue
		}

		log.Printf("connected from Client[%02d]:%s\n", try_id, conn.RemoteAddr().String())
		go handle(conn, try_id, prx_snd_chnl_slice[rq_map_idx], rq_map_slice[rq_map_idx], rq_map_idx)

	}
}

// --------------------------------------------------------------------

const (
	proxy_timeout = 20
	proxy_server  = "127.0.0.1:9090"
	// proxy_server = "172.21.46.104:60618"
	//proxy_server = "172.17.248.76:61618"

	local_ip   = "0.0.0.0"
	local_port = 1987
	//local_port = 9988
)

var (
	l_host string
	l_port int
	s_addr_list_input string
 
	log_lvl int 
//proxy_timeout = 20
)

func init() {
	flag.StringVar(&l_host, "l", "127.0.0.1", "Local IP")
	flag.IntVar(&l_port, "p", 1987, "Local listen Port")

	flag.StringVar(&s_addr_list_input, "s", "127.0.0.1:8091", "Remote addr list :: IP1:Port1,IP2:Port2")

	flag.IntVar(&log_lvl, "d", 2, "log lvl: 2")
}

var mutex sync.Mutex

func main() {

	flag.Parse()

	//runtime.GOMAXPROCS(SYS_CPU_NUM)

	//var lotusSNum int

	var s_addr_list []string
	s_addr_list = strings.Split(s_addr_list_input,",")

	if len(s_addr_list) > RQ_MAP_SLICE_SIZE{
		log.Printf("ERROR:Too many back server addr!, max num:%d\n", RQ_MAP_SLICE_SIZE);
		return 
	}

	for i:=0; i< len(s_addr_list); i++{
		log.Printf("s_addr_list[%d]:%s\n",i,s_addr_list[i])
	}

	if len(s_addr_list) > 0{
		TcpLotusMain(l_host, l_port, s_addr_list)		
	}else {
		log.Printf("Invalid para! Run with -h for help\n")
	}

}
