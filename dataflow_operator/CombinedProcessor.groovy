package xy.pars


class CombinedProcessor extends Processor {
  
	def processors
	
	CombinedProcessor(def processors){
		this.processors=processors
	}
	
	protected beforeInit(){
		processors*.beforeInit()
	}
	
	protected afterInit(){
		processors*.afterInit()
	}
	
	def onCompleted(){
		processors*.onCompleted()
	}
	
	Result process(Task task){
		Result result=null
		for(int i=0;i<processors.size();i++){
			result=processors[i].process(task)
			if(result!=null){
				if(result == Result.FILTER_OUT || result instanceof ExceptionResult){
					return result
				}
			}
		}
		return result
	}
}
