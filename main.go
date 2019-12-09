package main

import (
	"log"
	"os"
	"sync"
	// "time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/itsriyan/simple-consumers-sqs/controllers"
	"github.com/itsriyan/simple-consumers-sqs/models"
)

//define sqs message
type msg struct {
	msg []*sqs.Message
}

//define job with sqs message
type Job struct {
	id     int
	msgsqs *sqs.Message
}

//define result job
type Result struct {
	job Job
	err error
}

//jobs
var jobs = make(chan Job, 10)

//result
var results = make(chan Result, 10)

//define sessios conenct sqs aws
var sess = session.New(&aws.Config{
	Region:      aws.String(models.Region),
	Credentials: credentials.NewSharedCredentials(os.Getenv("AWS"), models.CredProfile),
	MaxRetries:  aws.Int(1),
})

//function worker membaca message dari sqs dan diproses kedalam controllers.ReadAssignment
func worker(wg *sync.WaitGroup) {
	//membaca job max 10 jobs karena sqs hanya dapat mengambil 10 message per call
	for job := range jobs {
		//output memproses hasil dari result dengan membaca controllers.ReadAssignment
		output := Result{job, controllers.ReadMsg(job.msgsqs)}
		results <- output
	}
	//sync Done
	wg.Done()
}

//func createWorkerPool menyiapkan worker
func createWorkerPool(noOfWorkers int) {
	// memulai wg.sync
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		//menjalani worker
		go worker(&wg)
	}
	wg.Wait()
	close(results)
}

//func allocate mengalokasi sqs message ke job
func allocate(msg []*sqs.Message) {

	for i, v := range msg {
		job := Job{i, v}
		jobs <- job
	}
	close(jobs)
}

//func result menangkap hasil dari worker
func result(done chan bool) {
	//svc membuat koneksi sqs baru
	svc := sqs.New(sess)
	//membaca result
	for result := range results {
		//print log
		// log.Println(" Job id: ", result.job.id, "Message SQS: ", result.job.msgsqs, "err: ", result.err)
		//membaca result job ada error atau tidaknya
		msgdel := result.job.msgsqs
		if result.err == nil {
			delete_params := &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(models.Url), // Required
				ReceiptHandle: msgdel.ReceiptHandle,   // Required
			}
			//bilah tidak ada error message akan dihapus
			_, err := svc.DeleteMessage(delete_params) // No response returned when successed.
			if err != nil {
				log.Println("Err : Delete msg : ", err)
			}
			// log.Println("LOG : Delete msg : ", *msgdel.MessageId)
		}
	}
	//job selesai
	done <- true
}
func main() {
	//for {} untuk infinity loop
	for {
		//memulai koneksi sqs
		svc := sqs.New(sess)

		//untuk menarik message sqs
		receive_params := &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(models.Url), //url sqs
			MaxNumberOfMessages: aws.Int64(3),           //maxnumber get message/call min = 1 maks = 10
			VisibilityTimeout:   aws.Int64(1),           //
			WaitTimeSeconds:     aws.Int64(1),
		}
		//memulai mengambil message
		receive_resp, err := svc.ReceiveMessage(receive_params)
		if err != nil {
			log.Println("Error : Get msg : ", err)
		}
		//define jumlah worker berdasarkan jumlah message
		noOfWorkers := len(receive_resp.Messages)
		//cek jumlah worker dipastikan lebih dari 0 jika hasilnya aplikasi sleep selama 60 detik
		if noOfWorkers > 0 {
			//jika worker lebih dari 10 / akan di hardcode menjadi 10
			if noOfWorkers > 10 {
				noOfWorkers = 10
			}
			//memulai menyiapkan job dengan nilai maksimal 10
			jobs = make(chan Job, 10)
			//memulai menyiapkan hasil dengan nilai maksimal 10
			results = make(chan Result, 10)
			//time to start
			// startTime := time.Now()
			//memulai job dengan mengirimkan message sqs
			go allocate(receive_resp.Messages)
			//define done apabila proses selesai
			done := make(chan bool)
			go result(done)
			//memulai membuat worker
			createWorkerPool(noOfWorkers)
			<-done
			//waktu selesai
			// endTime := time.Now()
			// diff := endTime.Sub(startTime)
			// log.Println("Total time taken ", diff.Seconds(), "seconds")
			// time.Sleep(2 * time.Second)
		}
	}
}
