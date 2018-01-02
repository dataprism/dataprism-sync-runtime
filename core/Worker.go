package core

type Worker interface {

	Run(done chan int, dataChannel chan Data, errorsChannel chan error)

}