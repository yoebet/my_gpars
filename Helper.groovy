package xy.z

import groovyx.gpars.*
import groovyx.gpars.group.*
import groovyx.gpars.dataflow.*

import xy.util.*

/**
 *
 */
class Helper {
	
	
	static DataflowVariable runTask(PGroup group,Closure task){
		runTask(group,null,task)
	}
	
	static DataflowVariable runTaskAndTimer(String name,PGroup group,Closure task){
		runTask (group,null,{
				CommonUtil.executeTimer(name,task)
			})
	}
	
	static DataflowVariable runTask(PGroup group, DataflowVariable exceptionDFV, Closure task){
		group.task {
			use([DateCategory,CalendarCategory,ExceptionCategory]){
				try{
					return task.call()
				}catch(e){
					try{
						e.printGroovyStackTrace()
						if(exceptionDFV!=null){
							exceptionDFV.bindSafely e
						}else{
							return e
						}
					}catch(e2){
						e2.printStackTrace()
					}
				}
				return null
			}
		}
	}
	
	static Closure wrapReactor(DataflowVariable exceptionDFV,Closure whenBound){
		return wrapWhenBound(exceptionDFV,whenBound)
	}
	
	static Closure wrapWhenBound(DataflowVariable exceptionDFV,Closure whenBound){
		return {any->
			use([DateCategory,CalendarCategory,ExceptionCategory]){
				try{
					return whenBound.call(any)
				}catch(e){
					try{
						e.printGroovyStackTrace()
						exceptionDFV?.bindSafely e
					}catch(e2){
						e2.printStackTrace()
					}
				}
				return null
			}
		}
	}
	
}

