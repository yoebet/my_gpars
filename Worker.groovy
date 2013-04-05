package xy.z.scene

import groovy.sql.Sql
import groovyx.gpars.*
import groovyx.gpars.actor.*
import groovyx.gpars.group.*
import groovyx.gpars.scheduler.*
import groovyx.gpars.dataflow.*

import xy.acc.*
import xy.bas.*
import xy.mod.*
import xy.util.*
import xy.cfcal.*
import xy.schema.*
import xy.data.mcf.*
import xy.data.scene.*
import xy.data.dat.NewVolume
import xy.cfcal.CalHelper
import xy.cfcal.ocf.OcfCalculator
import xy.common.DBConstant.RateMark
import xy.z.*
import static xy.z.Helper.*

/**
 *
 */
abstract class Worker {
	
	Scene scene
	
    PGroup group
	
    def dataSource
	
	Sql sql
	
	/********************/
	
	protected OcfCalculator ocfCalculator
	
	protected List<Date> cfDates
	
	protected Map<Date,Integer> monthEndToMonthIndex
	
	protected Date baseDate
	
	protected Date farthestDate
	
	protected AggregateManager aggregateManager
	
	protected ResultStore resultStore
	
	protected DataflowVariable workerStatusDFV
	
	
	DataflowVariable runTask(Closure task){
		runTask (group,workerStatusDFV,task)
	}
	
	Closure wrapReactor(Closure whenBound){
		wrapWhenBound(workerStatusDFV,whenBound)
	}
	
	Closure wrapWhenBound(Closure whenBound){
		wrapWhenBound(workerStatusDFV,whenBound)
	}
	
	def init(){
		
		baseDate=new Date(scene.baseDate.time)
		String baseDateStr=baseDate.format('yyyyMMdd')
		
		cfDates=[]
		monthEndToMonthIndex=[:]
		Date cfDate=baseDate
		scene.months.times {
			cfDate=cfDate.nextMonthEnd()
			cfDates << cfDate
			monthEndToMonthIndex[cfDate]=it+1
		}
		farthestDate=cfDates.last()
		
		println "baseDate: $baseDateStr"
		println "cfDates: ${cfDates.first().format('yyyyMMdd')} <=> ${farthestDate.format('yyyyMMdd')}"
		
		workerStatusDFV=new DataflowVariable()
		ocfCalculator=OcfCalculator.buildCalculator(NewVolume,baseDate)
		ocfCalculator.cacheRepriceDates=true
		ocfCalculator.toSetOcfRepriceDate=false
		ocfCalculator.farthestDate=farthestDate
		
		sql=new Sql(dataSource)
		aggregateManager=new AggregateManager(dataSource:dataSource,sql:sql,
			sceneId:scene.id,dynamicModelId:scene.dynamicModel.id,
			date:baseDateStr,logSql:false)
		aggregateManager.init()
		resultStore=new ResultStore(scene:scene,sql:sql,schemaManager:aggregateManager)
		
		if(group==null){
			group=new DefaultPGroup(new ResizeablePool(true))
		}
	}
	
	Actor setupCollectorFromMcfActor(DataflowVariable mcfsDFV,int waitCount=1){
		
		McfAggregator mcfAggregator=new McfAggregator()
		group.actor {
			loop {
				react wrapReactor({ mcf ->
						if(mcf==null){
							if((--waitCount)==0){
								List nvMcfs=mcfAggregator.done()
								mcfsDFV.bind nvMcfs
								delegate.terminate()
							}
							return
						}
						mcfAggregator.pushMcf(mcf)
					})
			}
		}
	}
	
	Actor setupMcfAggregatorActor(DataflowVariable mcfsDFV){
		
		int mcfsCount=0
		McfAggregator nvMcfAggregator=new McfAggregator()
		group.actor {
			loop {
				react wrapReactor({ task ->
						if(task==null){
							List nvMcfs=nvMcfAggregator.done()
							println "mcfsCount: ${mcfsCount}"
							println "mcfdsCount: ${nvMcfs.size()}"
							mcfsDFV.bind nvMcfs
							delegate.terminate()
							return
						}
						if(task.mcfs){
							//def nv=task.model
							//println "mcf#${task.id}: ${nv.originalDate.format('yyyyMMdd')},${nv.accitemId},${nv.accitemTermId},${nv.endingBalance}"
							task.mcfs.each { mcf->
								nvMcfAggregator.pushMcf(mcf)
								//print "  ${task.id}: ${mcf}"
							}
							mcfsCount+=task.mcfs.size()
							task.mcfs=null
						}
					})
			}
		}
	}
	
	Actor setupMcfAggregatorActor(Actor next){
		
		int mcfsCount=0
		McfAggregator nvMcfAggregator=new McfAggregator()
		group.actor {
			loop {
				react wrapReactor({ task ->
						if(task==null){
							List nvMcfs=nvMcfAggregator.done()
							println "mcfsCount: ${mcfsCount}"
							println "mcfdsCount: ${nvMcfs.size()}"
							nvMcfs.each{
								next << it
							}
							next << null
							delegate.terminate()
							return
						}
						if(task.mcfs){
							//def nv=task.model
							//println "mcf#${task.id}: ${nv.originalDate.format('yyyyMMdd')},${nv.accitemId},${nv.accitemTermId},${nv.endingBalance}"
							task.mcfs.each { mcf->
								nvMcfAggregator.pushMcf(mcf)
								//print "  ${task.id}: ${mcf}"
							}
							mcfsCount+=task.mcfs.size()
							task.mcfs=null
						}
					})
			}
		}
	}
	
	Actor setupRepriceSplitterActor(Actor next,DataflowVariable finalNewVolumesDFV){
		
		Map repriceStructuresByAccitemId=scene.goal?.repriceStructuresByAccitemId ?: [:]
		def finalNewVolumes=[]
		group.actor {
			loop {
				react wrapReactor({ task ->
						if(task==null){
							next << null
							delegate.terminate()
							println "finalNewVolumes: ${finalNewVolumes.size()}"
							finalNewVolumesDFV.bind finalNewVolumes
							return
						}
						List tasks=NewVolumeBuilder.splitTaskToReprices(repriceStructuresByAccitemId,task)
						tasks.each{
							next << it
							finalNewVolumes << it.model
						}
					})
			}
		}
	}
	
	
	DataflowVariable repriceStockMcfs(DataflowVariable stockMcfsDFV,Actor next,
		DataflowVariable rateDispositionMapDFV){
		
		Map<String,Reprice> repriceFrequencyMap=[:].withDefault{new Reprice(it)}
		Map<String,Date> lastRepriceDateMap=[:]
		Closure lastRepriceDateEvaluator={String repriceFrequency,Date thisRepriceDate->
			if(thisRepriceDate==null){
				return null
			}
			def key="${repriceFrequency}|${thisRepriceDate.format('yyyyMMdd')}"
			Date repriceDate=lastRepriceDateMap[key]
			if(repriceDate){
				println "hit repriceDate: $key"
				return repriceDate
			}
			Reprice reprice=repriceFrequencyMap[repriceFrequency]
			repriceDate=OcfCalculator.evaluateLastRepriceDate(thisRepriceDate,reprice)
			lastRepriceDateMap[key]=repriceDate
			return repriceDate
		}
		
		runTask {->
			List mcfs=stockMcfsDFV.get()
			println "stock mcfs reprice..."
			Map<Long,Map<Long,RateDisposition>> rateDispositionMap=rateDispositionMapDFV.get()
			
			mcfs.each{ mcf ->
				if(mcf.rateMark && mcf.rateMark!=RateMark.VARIABLE){
					try{
						def accitemId=mcf.accitemId
						def accitemTermId=mcf.accitemTermId
						def termRDMap=rateDispositionMap[accitemId]
						if(termRDMap){
							def rateDisposition=termRDMap[accitemTermId]
							if(rateDisposition){
								//重定价
								Reprices.repriceMcfd(mcf,rateDisposition,lastRepriceDateEvaluator)
							}
						}
					}catch(e){
						e.printGroovyStackTrace()
					}
				}
				
				//Mcfd => Mcft
				next << Mcfs.mcfdToMcft(mcf)
			}
			next << null
		}
	}
	
	Actor setupNvRepriceActor(Actor next){
		
		Map<Date,List> dateToRepriceEntryCache=[:].withDefault{
			//Date,Integer[]
			Reprices.monthEndAndRateEffectDays(it)
		}
		
		group.actor {
			loop {
				react wrapReactor({ task ->
						if(task==null){
							next << null
							delegate.terminate()
							return
						}
						
						AccItem accItem=task.accItem
						if(accItem.reprice && accItem.rateMark!=RateMark.FIXED){
							try{
								if(accItem.rateMark!=RateMark.VARIABLE){
									ocfCalculator.evaluateRepriceDates(task)
								}
								def repriceDates=task.repriceDates
								//重定价情况。key是重定价月份的月末日期
								//value为两个元素的数组：[旧利率有效的天数,新利率有效的天数]
								//TODO:
								Map repriceMap=repriceDates?.collectEntries{
									dateToRepriceEntryCache[it]
								}
								Reprices.repriceNv(task,repriceMap ?: [:])
							}catch(e){
								e.printGroovyStackTrace()
							}
						}
						
						next << task
					})
			}
		}
	}
	
	Actor setupOcfGeneratorActor(Actor next){
		
		int ocfsCount=0
		group.actor {
			loop {
				react wrapReactor({ task ->
						
						if(task==null){
							next << null
							delegate.terminate()
							println "ocfsCount: ${ocfsCount}"
							return
						}
						task.ocfs=ocfCalculator.evaluateBaseAndAccountChange(task,'nv')
						//						def nv=task.model
						//						println "ocf#${task.id}: ${nv.originalDate.format('yyyyMMdd')},${nv.accitemId},${nv.accitemTermId},${nv.endingBalance}"
						//						task.ocfs.each{
						//							print "  ${task.id}: ${it}"
						//						}
						ocfsCount+=task.ocfs.size()
						next << task
					})
			}
		}
	}
	
	Actor setupMcfGeneratorActor(Actor next){
		
		group.actor {
			loop {
				react wrapReactor({ task ->
						if(task==null){
							next << null
							delegate.terminate()
							return
						}
						CalHelper.generateMcf(task,farthestDate)
						task.ocfs=null
						next << task
					})
			}
		}
	}
	
	def buildAndDispatchTasks(Actor next,Map<AccItem,List<NewVolume>> newVolumes,Map rateDispositionMap){
		
		int nvCount=0
		
		Map accitemIdToRateSpread=null
		def rateSpreadSet=scene.rateSpreadSet
		if(rateSpreadSet){
			accitemIdToRateSpread=rateSpreadSet.rateSpreads.collectEntries{ rs->
				[rs.accitem.id,rs]
			}
		}
		
		newVolumes?.each{AccItem accitem,List<NewVolume> list->
			def termToRateMap=rateDispositionMap[accitem.id]
			def rateSpread=null
			if(accitemIdToRateSpread!=null){
				rateSpread=accitemIdToRateSpread[accitem.id]
			}
			list.eachWithIndex{ NewVolume nv,i->
				NvCfTask task=new NvCfTask(accItem:accitem,model:nv,id:nvCount+i)
				if(accitem.reprice){
					if(termToRateMap){
						task.rateDisposition=termToRateMap[nv.accitemTermId]
						//println "task rd: ${task.rateDisposition}"
					}
					task.rateSpread=rateSpread
				}
				next << task
			}
			nvCount+=list.size()
		}
		next << null
	}
	
	DataflowVariable loadRateDispositionMap(){
		runTask {->
			return Rates.buildRateDispositionMap(scene)
		}
	}
	
	DataflowVariable loadDiscountDispositionMap(){
		runTask {->
			return Rates.buildDiscountDispositionMap(scene)
		}
	}
	
	DataflowVariable loadErateDisposition(){
		runTask {->
			if(scene.currency=='01'){
				return null
			}
			return Exchanges.buildErateDisposition(scene)
		}
	}
	
	DataflowVariable stockMcfsLoader(){
		runTask {->
			return Mcfs.loadCfMcf(scene,farthestDate,sql)
		}
	}
	
	DataflowVariable storeInitialize(){
		runTask {->
			resultStore.init()
		}
	}
	
	DataflowVariable generateDurations(DataflowVariable discountDispositionMapDFV){
		
		runTask {->
			def mcfs=Mcfs.loadCfMcfForDuration(scene,sql)
			def dispositionMap=discountDispositionMapDFV.get()
			
			return Durations.calculateDuration(mcfs,baseDate,dispositionMap)
		}
	}
	
	DataflowVariable storeFinalMcfs(List finalMcfs,ErateDisposition ed){
		
		runTask {->
			println "finalMcfs: ${finalMcfs.size()}"
			
			resultStore.storeMcfs(finalMcfs)
			
			if(scene.currency!='01' && ed!=null){
				try{
					Exchanges.exMcfs(finalMcfs,ed)
					resultStore.storeHcMcfs(finalMcfs)
				}catch(e){
					e.printGroovyStackTrace()
				}
			}
			
			println "storeMcfs done."
		}
	}
	
	DataflowVariable storePlanNvs(List newVolumes,ErateDisposition ed){
		runTask {->
			def planNvs=newVolumes?.collect{ NewVolume nv ->
				Mcfs.nvToPlanNv(nv)
			}
			println "nvs: ${planNvs.size()}"
			resultStore.storePlanNvs(planNvs)
			
			if(scene.currency!='01' && ed!=null){
				try{
					Exchanges.exPlanNv(planNvs,ed)
					resultStore.storeHcPlanNvs(planNvs)
				}catch(e){
					e.printGroovyStackTrace()
				}
			}
			println "storePlanNvs done."
		}
	}
	
	DataflowVariable sumDataTable(){
		runTask {->
			//sql.withTransaction {
			aggregateManager.sumSceMcfTerm()
			aggregateManager.sumSceMcf()
			aggregateManager.sumSceMcfRep()
			aggregateManager.sumSceNv()
			if(scene.currency!='01'){
				aggregateManager.sumSceHcMcfTerm()
				aggregateManager.sumSceHcMcf()
				aggregateManager.sumSceHcMcfRep()
				aggregateManager.sumSceHcNv()
			}
			//}
		}
	}
	
	
	abstract DataflowVariable start()
	
	void close() {
		sql?.close()
		aggregateManager?.close()
	}
}

