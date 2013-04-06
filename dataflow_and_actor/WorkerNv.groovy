package xy.z.scene

import groovyx.gpars.*
import groovyx.gpars.actor.*
import groovyx.gpars.group.*
import groovyx.gpars.dataflow.*

import xy.acc.*
import xy.bas.*
import xy.mod.*
import xy.util.*
import xy.data.mcf.*
import xy.data.scene.*
import xy.data.dat.NewVolume
import xy.cfcal.CalHelper
import xy.cfcal.ocf.OcfCalculator
import xy.z.scene.NewVolumeBuilder
import xy.z.*
import static xy.z.Helper.*

/**
 * 新业务量情景模拟
 */
class WorkerNv extends Worker {
	
	def init(){
		
		def goal=scene.goal
		if(goal){
			//1：新业务量；2：目标余额；3：增长比例
			if(goal.goalType!=1){
				throw new RuntimeException("业务目标类型不是新业务量目标")
			}
		}
		
		super.init()
	}
	
	DataflowVariable buildNewVolumes(Actor next,DataflowVariable rateDispositionMapDFV){
		runTask {->
			println "build new volumes..."
			Map<AccItem,List<NewVolume>> newVolumes=NewVolumeBuilder.buildNewVolumesBasic(scene,cfDates)
			Map rateDispositionMap=rateDispositionMapDFV.get()
			newVolumes=NewVolumeBuilder.splitNewVolumeToTerms(scene,newVolumes,rateDispositionMap)
			buildAndDispatchTasks(next,newVolumes,rateDispositionMap)
			return true
		}
	}
	
	private DataflowVariable storeResult(DataflowVariable stockMcfsDFV,
		DataflowVariable finalMcfsDFV,DataflowVariable finalNewVolumesDFV,
		DataflowVariable erateDispositionDFV,DataflowVariable durationsDFV){
		
		runTask {->
			DataflowVariable storeInitialized=storeInitialize()
			storeInitialized.join()
			
			def erateDisposition=erateDispositionDFV.get()
			
			//存储结果月现金流
			List stockMcfs=stockMcfsDFV.get()
			def fdt=cfDates[0].time
			def baseDateMcfs=stockMcfs.findAll{
				it.cfDate.time < fdt
			}
			List allFinalMcfs=baseDateMcfs.collect{
				Mcfs.mcfdToMcft(it)
			}
			List finalMcfs=finalMcfsDFV.get()
			allFinalMcfs.addAll(finalMcfs)
			def sfm=storeFinalMcfs(allFinalMcfs,erateDisposition)
			//存储新业务量
			List newVolumes=finalNewVolumesDFV.get()
			def spn=storePlanNvs(newVolumes,erateDisposition)
			sfm.join()
			spn.join()
			
			def durations=durationsDFV.get()
			resultStore.storeDurations(durations)
		}
	}
	
	
	DataflowVariable start(){
		
		println "workerNv is starting..."
		
		//利率模拟，accitemId -> termId -> rateDisposition
		DataflowVariable rateDispositionMapDFV=loadRateDispositionMap()
		
		//折现率，accitemId -> termId -> discountDisposition
		DataflowVariable discountDispositionMapDFV=loadDiscountDispositionMap()
		
		//计算市值与久期
		DataflowVariable durationsDFV=generateDurations(discountDispositionMapDFV)
		
		//汇率模拟
		DataflowVariable erateDispositionDFV=loadErateDisposition()
		
		//新业务量
		DataflowVariable finalNewVolumesDFV=new DataflowVariable()
		
		//最终月现金流（合并了存量数据）
		DataflowVariable finalMcfsDFV=new DataflowVariable()
		
		//接收已重定价月现金流，并合并
		Actor finalMcfsCollectorActor=setupCollectorFromMcfActor(finalMcfsDFV,2)
		
		//汇总基础月现金流
		Actor nvMcfAggregatorActor=setupMcfAggregatorActor(finalMcfsCollectorActor)
		
		//新业务量重定价
		Actor nvRepriceActor=setupNvRepriceActor(nvMcfAggregatorActor)
		
		//分重定价
		Actor repriceSplitterActor=setupRepriceSplitterActor(nvRepriceActor,finalNewVolumesDFV)
		
		//生成基础月现金流
		Actor mcfGeneratorActor=setupMcfGeneratorActor(repriceSplitterActor)
		
		//生成新业务量发生现金流
		Actor ocfGeneratorActor=setupOcfGeneratorActor(mcfGeneratorActor)
		
		//新业务量（未分重定价）
		buildNewVolumes(ocfGeneratorActor,rateDispositionMapDFV)
		
		//加载存量业务
		DataflowVariable stockMcfsDFV=stockMcfsLoader()
		
		//重定价：存量业务
		repriceStockMcfs(stockMcfsDFV,finalMcfsCollectorActor,rateDispositionMapDFV)
		
		/*** 存储结果 ***/
		
		runTask {->
			def storeResultDFV=storeResult(stockMcfsDFV,finalMcfsDFV,
				finalNewVolumesDFV,erateDispositionDFV,durationsDFV)
			storeResultDFV.join()
			def sdtDFV=sumDataTable()
			sdtDFV.join()
			workerStatusDFV.bindSafely 'done'
		}
		
		return workerStatusDFV
	}
	
}

