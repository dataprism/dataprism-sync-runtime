package core

type InputWorker interface {

	Run(done chan int, dataChannel chan Data, errorsChannel chan error)

}

type OutputWorker interface {

	Run(done chan int, dataChannel chan []Data, errorsChannel chan error)

}