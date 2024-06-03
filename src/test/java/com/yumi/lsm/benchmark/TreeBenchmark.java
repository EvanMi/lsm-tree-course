package com.yumi.lsm.benchmark;

import com.yumi.lsm.Config;
import com.yumi.lsm.Tree;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.yumi.lsm.TreeTestHelper.cleanFolder;

@State(Scope.Benchmark)
public class TreeBenchmark {

    private  Tree tree;
    static final String keyPrefix = "preFix";

    @Setup
    public void setup() {
        tree = new Tree(Config.newConfig("/tmp/yumi"));
    }

    @TearDown
    public void tearDown() {
        tree.close();
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(8)
    public void putAndGet() {
        String key = keyPrefix +  ThreadLocalRandom.current().nextInt(10000000);
        tree.put(key.getBytes(), key.getBytes());
        key = keyPrefix +  ThreadLocalRandom.current().nextInt(10000000);
        tree.get(key.getBytes());
    }

    public static void main(String[] args) throws RunnerException {
        String workDir = "/tmp/yumi";
        File file = new File(workDir);
        cleanFolder(file);
        file.delete();


        Options opt = new OptionsBuilder()
                .include(TreeBenchmark.class.getSimpleName())
                .forks(1)
                .measurementIterations(60)
                .build();
        new Runner(opt).run();
    }
}
