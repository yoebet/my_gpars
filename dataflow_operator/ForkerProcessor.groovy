package xy.pars

import xy.pars.message.*


class ForkerProcessor extends Processor {
	
	protected List<Processor> forkSuccessors
  
	void setForkSuccessors(List<Processor> forkSuccessors){
		this.forkSuccessors=forkSuccessors
		forkSuccessors.each{
			it.previous << this
		}
	}
	
	def beforeInit(){
	}

	Result process(Task task){

		forkSuccessors?.each{ successor ->
			def successorQueue=successor.tasksQueue
			while(successorQueue.length() >= Processor.MAX_QUEUE_LENGTH){
				Thread.yield()
			}
			successorQueue << task
		}

		return null
	}

	def onCompleted(){
		def completedMessage=new Completed(sender:this)
		forkSuccessors?.each { it << completedMessage }
		super.onCompleted()
	}

}
