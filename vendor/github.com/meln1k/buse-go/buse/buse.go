package buse

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"syscall"
)

func ioctl(fd, op, arg uintptr) {
	_, _, ep := syscall.Syscall(syscall.SYS_IOCTL, fd, op, arg)
	if ep != 0 {
		log.Fatalf("ioctl(%d, %d, %d) failed: %s", fd, op, arg, syscall.Errno(ep))
	}
}

func opDeviceRead(driver BuseInterface, fp *os.File, mutex *sync.Mutex, chunk []byte, request *nbdRequest) error {
	var nbdErrorReplyCode uint32
	if err := driver.ReadAt(chunk, request.Offset); err != nil {
		log.Println("buseDriver.WriteAt returned an error:", err)
		nbdErr, ok := err.(NbdError)
		if ok {
			nbdErrorReplyCode = nbdErr.value
		} else { // not an NbdError, shouldn't happen in general
			nbdErrorReplyCode = EPERM
		}
	}
	header := nbdReplyHeader(request.Handle, nbdErrorReplyCode)
	mutex.Lock()
	defer mutex.Unlock()
	if _, err := fp.Write(header); err != nil {
		log.Println("Write error, when sending reply header:", err)
	}
	if _, err := fp.Write(chunk); err != nil {
		log.Println("Write error, when sending data chunk:", err)
	}
	return nil
}

func opDeviceWrite(driver BuseInterface, fp *os.File, mutex *sync.Mutex, chunk []byte, request *nbdRequest) error {
	var nbdErrorReplyCode uint32
	if err := driver.WriteAt(chunk, request.Offset); err != nil {
		log.Println("buseDriver.WriteAt returned an error:", err)
		nbdErr, ok := err.(NbdError)
		if ok {
			nbdErrorReplyCode = nbdErr.value
		} else { // not an NbdError, shouldn't happen in general
			nbdErrorReplyCode = EPERM
		}
	}
	header := nbdReplyHeader(request.Handle, nbdErrorReplyCode)
	mutex.Lock()
	defer mutex.Unlock()
	if _, err := fp.Write(header); err != nil {
		log.Println("Write error, when sending reply header:", err)
	}
	return nil
}

func opDeviceDisconnect(driver BuseInterface, fp *os.File, mutex *sync.Mutex, chunk []byte, request *nbdRequest) error {
	log.Println("Calling buseDriver.Disconnect()")
	driver.Disconnect()
	return fmt.Errorf("Received a disconnect")
}

func opDeviceFlush(driver BuseInterface, fp *os.File, mutex *sync.Mutex, chunk []byte, request *nbdRequest) error {
	var nbdErrorReplyCode uint32
	if err := driver.Flush(); err != nil {
		log.Println("buseDriver.WriteAt returned an error:", err)
		nbdErr, ok := err.(NbdError)
		if ok {
			nbdErrorReplyCode = nbdErr.value
		} else { // not an NbdError, shouldn't happen in general
			nbdErrorReplyCode = EPERM
		}
	}
	header := nbdReplyHeader(request.Handle, nbdErrorReplyCode)
	mutex.Lock()
	defer mutex.Unlock()
	if _, err := fp.Write(header); err != nil {
		log.Println("Write error, when sending reply header:", err)
	}
	return nil
}

func opDeviceTrim(driver BuseInterface, fp *os.File, mutex *sync.Mutex, chunk []byte, request *nbdRequest) error {
	var nbdErrorReplyCode uint32
	if err := driver.Trim(request.Offset, request.Length); err != nil {
		log.Println("buseDriver.WriteAt returned an error:", err)
		nbdErr, ok := err.(NbdError)
		if ok {
			nbdErrorReplyCode = nbdErr.value
		} else { // not an NbdError, shouldn't happen in general
			nbdErrorReplyCode = EPERM
		}
	}
	header := nbdReplyHeader(request.Handle, nbdErrorReplyCode)
	mutex.Lock()
	defer mutex.Unlock()
	if _, err := fp.Write(header); err != nil {
		log.Println("Write error, when sending reply header:", err)
	}
	return nil
}

func (bd *BuseDevice) startNBDClient() {
	ioctl(bd.deviceFp.Fd(), NBD_SET_SOCK, uintptr(bd.socketPair[1]))
	// The call below may fail on some systems (if flags unset), could be ignored
	ioctl(bd.deviceFp.Fd(), NBD_SET_FLAGS, NBD_FLAG_SEND_TRIM)
	// The following call will block until the client disconnects
	log.Println("Starting NBD client...")
	go ioctl(bd.deviceFp.Fd(), NBD_DO_IT, 0)
	// Block on the disconnect channel
	<-bd.disconnect
}

// Disconnect disconnects the BuseDevice
func (bd *BuseDevice) Disconnect() {
	bd.disconnect <- 1
	// Ok to fail, ignore errors
	syscall.Syscall(syscall.SYS_IOCTL, bd.deviceFp.Fd(), NBD_CLEAR_QUE, 0)
	syscall.Syscall(syscall.SYS_IOCTL, bd.deviceFp.Fd(), NBD_DISCONNECT, 0)
	syscall.Syscall(syscall.SYS_IOCTL, bd.deviceFp.Fd(), NBD_CLEAR_SOCK, 0)
	// Cleanup fd
	syscall.Close(bd.socketPair[0])
	syscall.Close(bd.socketPair[1])
	bd.deviceFp.Close()
	log.Println("NBD client disconnected")
}

func readNbdRequest(buf []byte, request *nbdRequest) {
	request.Magic = binary.BigEndian.Uint32(buf)
	request.Type = binary.BigEndian.Uint32(buf[4:8])
	request.Handle = binary.BigEndian.Uint64(buf[8:16])
	request.Offset = binary.BigEndian.Uint64(buf[16:24])
	request.Length = binary.BigEndian.Uint32(buf[24:28])
}

func nbdReplyHeader(handle uint64, error uint32) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], NBD_REPLY_MAGIC)
	binary.BigEndian.PutUint32(buf[4:8], error)
	binary.BigEndian.PutUint64(buf[8:16], handle)
	// NOTE: a struct in go has 4 extra bytes, so we skip the last
	return buf
}

// Connect connects a BuseDevice to an actual device file
// and starts handling requests. It does not return until it's done serving requests.
func (bd *BuseDevice) Connect() error {
	go bd.startNBDClient()
	defer bd.Disconnect()
	//opens the device file at least once, to make sure the partition table is updated
	tmp, err := os.Open(bd.device)
	if err != nil {
		return fmt.Errorf("Cannot reach the device %s: %s", bd.device, err)
	}
	tmp.Close()
	fp := os.NewFile(uintptr(bd.socketPair[0]), "unix")
	mutex := &sync.Mutex{}

	// Start handling requests
	for true {
		request := nbdRequest{}
		// NOTE: a struct in go has 4 extra bytes...
		buf := make([]byte, 28)
		if _, err := fp.Read(buf[0:28]); err != nil {
			return fmt.Errorf("NBD client stopped: %s", err)
		}

		readNbdRequest(buf, &request)
		// fmt.Printf("DEBUG %#v\n", request)
		if request.Magic != NBD_REQUEST_MAGIC {
			return fmt.Errorf("Fatal error: received packet with wrong Magic number")
		}
		reply := nbdReply{Magic: NBD_REPLY_MAGIC}
		reply.Handle = request.Handle
		chunk := make([]byte, request.Length)
		reply.Error = 0
		// Dispatches READ, WRITE, DISC, FLUSH, TRIM to the corresponding implementation
		if request.Type < NBD_CMD_READ || request.Type > NBD_CMD_TRIM {
			log.Println("Received unknown request:", request.Type)
			continue
		}
		if request.Type == NBD_CMD_WRITE {
			if _, err := io.ReadFull(fp, chunk); err != nil {
				return fmt.Errorf("Fatal error, cannot read request packet: %s", err)
			}
		}

		// asynchronously handle the rest
		go bd.op[request.Type](bd.driver, fp, mutex, chunk, &request)
	}
	return nil
}

func CreateDevice(device string, blockSize int32, size uint64, buseDriver BuseInterface) (*BuseDevice, error) {
	buseDevice := &BuseDevice{size: size, device: device, driver: buseDriver}
	sockPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, fmt.Errorf("Call to socketpair failed: %s", err)
	}
	fp, err := os.OpenFile(device, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("Cannot open \"%s\". Make sure the `nbd' kernel module is loaded: %s", device, err)
	}
	buseDevice.deviceFp = fp
	ioctl(buseDevice.deviceFp.Fd(), NBD_SET_SIZE, uintptr(size))
	ioctl(buseDevice.deviceFp.Fd(), NBD_SET_BLKSIZE, uintptr(blockSize))
	ioctl(buseDevice.deviceFp.Fd(), NBD_CLEAR_QUE, 0)
	ioctl(buseDevice.deviceFp.Fd(), NBD_CLEAR_SOCK, 0)
	buseDevice.socketPair = sockPair
	buseDevice.op[NBD_CMD_READ] = opDeviceRead
	buseDevice.op[NBD_CMD_WRITE] = opDeviceWrite
	buseDevice.op[NBD_CMD_DISC] = opDeviceDisconnect
	buseDevice.op[NBD_CMD_FLUSH] = opDeviceFlush
	buseDevice.op[NBD_CMD_TRIM] = opDeviceTrim
	buseDevice.disconnect = make(chan int, 5)
	return buseDevice, nil
}
