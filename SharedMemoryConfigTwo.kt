import java.io.BufferedWriter
import java.io.FileWriter
import java.io.IOException
import java.io.EOFException
import java.io.FileReader
import java.io.BufferedReader
import java.io.File

import java .util.LinkedList
import java.util.concurrent.Executors
import java.util.Comparator
import java.util.PriorityQueue
import java.util.concurrent.CountDownLatch

val THREAD_POOL_SIZE = 8
val workingDirectory = File(System.getProperty("user.dir"))
val resultFile = File(workingDirectory, "Output.txt")
val tempDir = File(workingDirectory, "temp/")
//val BLOCK_SIZE = 500000000
var start = System.currentTimeMillis()
val rawFilesQueue = LinkedList<File>()
fun main(args: Array<String>) {
    val inputFile = File(workingDirectory, "test.txt")

    breakInput(inputFile, tempDir)

    System.gc()

    start = System.currentTimeMillis()

    sortFiles(workingDirectory)
}

fun breakInput(inpFile: File, tempDir: File) {

    tempDir.apply {
        if (!exists()) {
            mkdir()
        }
    }

    val data = ArrayList<String>()

    var fileCounter = 0
//    val BLOCK_SIZE = Runtime.getRuntime().freeMemory() / 2
    val BLOCK_SIZE = 20000000


    try {
        val reader = BufferedReader(FileReader(inpFile))
        var line: String? = ""

        while (line != null) {
            var currentblocksize = 0// in bytes
            do {
                line = reader.readLine()
                if (line != null) {
                    data.add(line)
                    currentblocksize += line.length
                }
            } while (currentblocksize < BLOCK_SIZE && line != null)

            File(tempDir, "$fileCounter.txt").bufferedWriter().use { writer ->
                data.forEach { d ->
                    writer.write(d)
                    writer.write("\n")
                }
            }
            fileCounter++
            data.clear()
            System.gc()
        }

        File(workingDirectory, "data stats").bufferedWriter().use {
            it.write("Block Size: $BLOCK_SIZE")
            it.newLine()
            it.write("Total Parts Created: ${tempDir.listFiles().size}")
            it.newLine()
        }

    } catch (e: Exception) {
        println(e)
    }
}
val latch = CountDownLatch(THREAD_POOL_SIZE)

fun sortFiles(workingDirectory: File) {

    rawFilesQueue.apply {
        tempDir.listFiles().forEach {
            this.add(it)
        }
    }

    File(tempDir, "classifiedParts/").apply {
        if (!this.exists()) {
            this.mkdir()
        }
    }


    val executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE)
    executorService.execute {
            run {
                0.until(THREAD_POOL_SIZE).forEach {
                    val thr = Thread(CustomThreadImplSmallFiles())
                    thr.start()
                }
                latch.await()

//                rawFilesQueue.clear()

                println("done small files")

                0.until(THREAD_POOL_SIZE).forEach {
                    val thr = Thread(customThreadImp())
                    thr.start()
                }
                val endTime = System.currentTimeMillis()
                File(workingDirectory, "Stats.txt").bufferedWriter().use {
                    it.write("Start Time: $start")
                    it.newLine()
                    it.write("End Time: $endTime")
                    it.newLine()
                    it.write("elapsed Time: ${(endTime - start)}")
                    it.newLine()
                }
            }

    }
    executorService.shutdown()
}

class CustomThreadImplSmallFiles(): Runnable {
    val inputDir = File(tempDir, "classifiedParts/")

    override fun run() {
        while (rawFilesQueue.peek() != null) {
            val file = rawFilesQueue.poll()
            val classLines = rAS(file)
            writeMultiParts(classLines, inputDir, file.name)
            file.delete()
        }
        latch.countDown()
    }
}

fun rAS(rawFile: File): ArrayList<String> {

    val ln = ArrayList<String>()
    rawFile.bufferedReader().use { br ->
        br.forEachLine { line ->
            ln.add(line)
        }
    }
    return classifyLines(ln)

}

fun classifyLines(ln: ArrayList<String>): ArrayList<String> {
    val keys = ArrayList<String>()
    val properLines = ArrayList<String>()
    val allData = HashMap<String, String>()

    ln.forEach { l ->
//        println(l)
        val k = l.substring(0, 10)
        keys.add(k)
        allData.put(k, l.substring(10, l.length))
    }
    ln.clear()
    val properKeys = customMergeSortImplementation(keys)
//    keys.clear()
//    println(properKeys.size)

    properKeys.forEach {
        properLines.add(it + " " + allData.get(it))
//        println(it + " " + allData.get(it))

    }

    properKeys.clear()
//    println(properLines.size)
    return properLines
}

fun writeMultiParts(classifiedLines: ArrayList<String>, classifiedFileDir: File, name: String) {

    File(classifiedFileDir, "$name.txt").bufferedWriter().use { writer ->
        classifiedLines.forEach {
//            println(it)
            writer.write(it)
            writer.newLine()
        }
    }
}

fun customMergeSortImplementation(inputData: ArrayList<String>): ArrayList<String> {

    val leftPartOdData: ArrayList<String>
    val rightPartOfData: ArrayList<String>

    if (inputData.size == 1) {
        return inputData
    } else {
        val middleOfArray: Int = (inputData.size / 2)

        val firstPartOfArray = ArrayList<String>().apply {
            0.until(middleOfArray).forEach { element ->
                this.add(inputData[element])
            }
        }

        val secondPartOfArray = ArrayList<String>().apply {
            middleOfArray.until(inputData.size).forEach { element ->
                this.add(inputData[element])
            }
        }

        leftPartOdData = customMergeSortImplementation(firstPartOfArray)
        rightPartOfData = customMergeSortImplementation(secondPartOfArray)

    }
    return customMerge(leftPartOdData, rightPartOfData, inputData)
}

fun customMerge(leftPartOfData: ArrayList<String>, rightPartOfData: ArrayList<String>, inputData: ArrayList<String>): ArrayList<String> {
    var lInd = 0
    var rInd = 0
    var dInd = 0

    while (lInd < leftPartOfData.size && rInd < rightPartOfData.size) {
        if ((leftPartOfData.get(lInd).compareTo(rightPartOfData.get(rInd))) < 0) {
            inputData.set(dInd, leftPartOfData.get(lInd))
            lInd++
        } else {
            inputData.set(dInd, rightPartOfData.get(rInd))
            rInd++
        }
        dInd++
    }

    var leftOverInd = 0

    val leftOver = ArrayList<String>().let {
        if (lInd >= leftPartOfData.size) {
            leftOverInd = rInd
            rightPartOfData
        } else {
            leftOverInd = lInd
            leftPartOfData
        }
    }

    leftOverInd.until(leftOver.size).forEach {
        inputData.set(dInd, leftOver.get(it))
        dInd++
    }

    return inputData
}

fun customCompareImpl(readerOne: String, readerTwo: String): Int {
    val lineOne = readerOne
    val lineTwo = readerTwo

    return lineOne.compareTo(lineTwo)

}

class customThreadImp: Runnable {
    override fun run() {
        combineClassifiedFiles()
    }
}

fun combineClassifiedFiles() {

    val files = ArrayList<File>().apply {
        File(tempDir, "classifiedParts/").listFiles().forEach {
            this.add(it)
        }
    }

    val pq = PriorityQueue<CustomFileBuffer>(11,
                Comparator<CustomFileBuffer>() { a, b -> customCompareImpl(a.fileReadCheck()!!, b.fileReadCheck()!!) }
        )

    files.forEach {
        pq.add(CustomFileBuffer(it))
    }

    files.clear()

    if (pq.size > 0) {
        val outputWriter = BufferedWriter(FileWriter(resultFile))
        try {
            while (pq.size > 0) {
                val buffRead = pq.poll()
                val r = buffRead.fileRead()
                outputWriter.write(r)
                outputWriter.newLine()
                if (buffRead.fileCheck()) {
                    buffRead.fileFinishReading()
                    buffRead.rawUnclassifiedFile.delete()

                } else {
                    pq.add(buffRead)
                }
            }
        } finally {
            outputWriter.close()
            for (bf in pq) bf.fileFinishReading()
        }
    }
}


class CustomFileBuffer @Throws(IOException::class)
constructor(var rawUnclassifiedFile: File) {
    var customFileBufferedReader: BufferedReader
    private var fileData: String? = null
    private var fileDataEmpty: Boolean = false

    init {
        customFileBufferedReader = BufferedReader(FileReader(rawUnclassifiedFile), CUSTOM_FILE_BUFFER_SIZE)
        fileRefresh()
    }

    fun fileCheck(): Boolean {
        return fileDataEmpty
    }

    @Throws(IOException::class)
    private fun fileRefresh() {
        try {
            fileData = customFileBufferedReader.readLine()
            if (fileData == null) {
                fileDataEmpty = true
                fileData = null
            } else {
                fileDataEmpty = false
            }
        } catch (oef: EOFException) {
            fileDataEmpty = true
            fileData = null
        }

    }

    @Throws(IOException::class)
    fun fileFinishReading() {
        customFileBufferedReader.close()
    }

    fun fileReadCheck(): String? {
        return if (fileCheck()) null else fileData!!.toString()
    }

    @Throws(IOException::class)
    fun fileRead(): String? {
        val answer = fileReadCheck()
        fileRefresh()
        return answer
    }

    companion object {
        var CUSTOM_FILE_BUFFER_SIZE = 10240
    }
}

