package core

type InputWorker interface {

	Run(done chan int, toOutput chan Data)

}

type OutputWorker interface {

	Run(done chan int, fromInput chan []Data)

}