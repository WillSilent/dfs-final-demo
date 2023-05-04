package db

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SCPConnectionPool struct {
	Address         string
	Username        string
	PrivateKeyPath  string
	PrivateKeyPass  string
	ConnectionLimit int
	connections     chan *sftp.Client
	mutex           sync.Mutex
}

func NewSCPConnectionPool(address, username, privateKeyPath, privateKeyPass string, connectionLimit int) (*SCPConnectionPool, error) {
	pool := &SCPConnectionPool{
		Address:         address,
		Username:        username,
		PrivateKeyPath:  privateKeyPath,
		PrivateKeyPass:  privateKeyPass,
		ConnectionLimit: connectionLimit,
		connections:     make(chan *sftp.Client, connectionLimit),
	}

	for i := 0; i < connectionLimit; i++ {
		client, err := pool.createClient()
		if err != nil {
			pool.Close()
			return nil, err
		}
		pool.connections <- client
	}

	return pool, nil
}

func (pool *SCPConnectionPool) createClient() (*sftp.Client, error) {
	privateKeyBytes, err := ioutil.ReadFile(pool.PrivateKeyPath)
	if err != nil {
		return nil, err
	}

	var signer ssh.Signer
	if pool.PrivateKeyPass != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(privateKeyBytes, []byte(pool.PrivateKeyPass))
	} else {
		signer, err = ssh.ParsePrivateKey(privateKeyBytes)
	}
	if err != nil {
		return nil, err
	}

	config := &ssh.ClientConfig{
		User: pool.Username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	client, err := ssh.Dial("tcp", pool.Address+":22", config)
	if err != nil {
		return nil, err
	}

	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		return nil, err
	}

	return sftpClient, nil
}

func (pool *SCPConnectionPool) GetConnection() (*sftp.Client, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	select {
	case conn := <-pool.connections:
		return conn, nil
	default:
		return pool.createClient()
	}
}

func (pool *SCPConnectionPool) ReturnConnection(conn *sftp.Client) {
	if conn == nil {
		return
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	select {
	case pool.connections <- conn:
	default:
		conn.Close()
	}
}

func (pool *SCPConnectionPool) Close() {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	close(pool.connections)
	for conn := range pool.connections {
		conn.Close()
	}
}

func main() {
	remoteHost := "35.236.240.242"
	remoteUser := "root"
	privateKeyPath := "/Users/shudong/Downloads/keys"
	privateKeyPassword := "123456"
	connectionsCount := 5

	scpPool, err := NewSCPConnectionPool(remoteHost, remoteUser, privateKeyPath, privateKeyPassword, connectionsCount)
	if err != nil {
		log.Fatal("Failed to create SCP connection pool:", err)
	}
	defer scpPool.Close()

	for i := 0; i < 120; i++ {
		sourcePath := fmt.Sprintf("/Users/shudong/Downloads/chunk_%d.bin", i)
		destinationPath := fmt.Sprintf("/chunk/chunk_%d.bin", i)

		sftpClient, err := scpPool.GetConnection()
		if err != nil {
			log.Fatal("Failed to get SCP connection:", err)
		}

		go func(sourcePath, destinationPath string, sftpClient *sftp.Client) {
			defer scpPool.ReturnConnection(sftpClient)

			sourceFile, err := os.Open(sourcePath)
			if err != nil {
				log.Fatal("Failed to open local file:", err)
			}
			defer sourceFile.Close()

			destinationFile, err := sftpClient.Create(destinationPath)
			if err != nil {
				log.Fatal("Failed to create remote file:", err)
			}
			defer destinationFile.Close()

			_, err = io.Copy(destinationFile, sourceFile)
			if err != nil {
				log.Fatal("Failed to copy file:", err)
			}

			fmt.Println("File", sourcePath, "transferred successfully!")
		}(sourcePath, destinationPath, sftpClient)
	}

	// 等待所有文件传输完成
	time.Sleep(time.Second * 5)
	fmt.Println("All files transferred!")
}
