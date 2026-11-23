package utb.fai;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class App {

    private static final int DEFAULT_MAX_DEPTH = 2;
    private static final int DEFAULT_DEBUG_LEVEL = 0;
    private static final int TOP_WORDS = 20;

    public static void main(String[] args) {
        if (args.length < 1 || args[0].isBlank()) {
            System.err.println("Missing parameter - start URL");
            return;
        }

        int maxDepth = DEFAULT_MAX_DEPTH;
        int debugLevel = DEFAULT_DEBUG_LEVEL;

        if (args.length >= 2) {
            try {
                maxDepth = Integer.parseInt(args[1]);
                if (maxDepth < 0) {
                    maxDepth = 0;
                }
            } catch (NumberFormatException e) {
                System.err.println("Neplatný parametr maxDepth, používám " + DEFAULT_MAX_DEPTH);
            }
        }

        if (args.length >= 3) {
            try {
                debugLevel = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Neplatný parametr debugLevel, používám " + DEFAULT_DEBUG_LEVEL);
            }
        }

        URI startUri = normalizeStartUri(args[0].trim());
        if (startUri == null) {
            System.err.println("Neplatné start URL: " + args[0]);
            return;
        }

        ConcurrentHashMap<URI, Boolean> visitedURIs = new ConcurrentHashMap<>();
        BlockingQueue<URIinfo> foundURIs = new LinkedBlockingQueue<>();
        AtomicInteger activeTasks = new AtomicInteger(0);

        ParserCallback callBack = new ParserCallback(visitedURIs, foundURIs);
        callBack.maxDepth = maxDepth;
        callBack.debugLevel = debugLevel;

        int threads = Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors()));
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        if (visitedURIs.putIfAbsent(startUri, Boolean.TRUE) == null) {
            foundURIs.offer(new URIinfo(startUri, 0));
        }

        try {
            long lastActivity = System.currentTimeMillis();
            final long maxIdleMs = 15_000L;

            while (true) {
                URIinfo info = foundURIs.poll(200, TimeUnit.MILLISECONDS);
                if (info != null) {
                    lastActivity = System.currentTimeMillis();
                    activeTasks.incrementAndGet();
                    executor.submit(() -> {
                        try {
                            callBack.createTask(info.uri, info.depth).run();
                        } finally {
                            activeTasks.decrementAndGet();
                        }
                    });
                } else {
                    if (foundURIs.isEmpty() && activeTasks.get() == 0) {
                        break;
                    }
                    if (System.currentTimeMillis() - lastActivity > maxIdleMs) {
                        if (debugLevel > 0) {
                            System.err.println("Ukončuji po timeoutu nečinnosti.");
                        }
                        break;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        List<Map.Entry<String, Integer>> topWords = callBack.foundWords.entrySet().stream()
            .sorted(Comparator.<Map.Entry<String, Integer>>comparingInt(Map.Entry::getValue).reversed()
                        .thenComparing(Map.Entry::getKey))
                .limit(TOP_WORDS)
                .toList();

        for (Map.Entry<String, Integer> entry : topWords) {
            System.out.println(entry.getKey() + ";" + entry.getValue());
        }
    }

    private static URI normalizeStartUri(String raw) {
        try {
            String withScheme = raw.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://.*") ? raw : "http://" + raw;
            URI uri = URI.create(withScheme);
            if (uri.getScheme() == null || uri.getHost() == null) {
                return null;
            }
            String path = (uri.getPath() == null || uri.getPath().isEmpty()) ? "/" : uri.getPath();
            return new URI(
                    uri.getScheme().toLowerCase(Locale.ROOT),
                    uri.getUserInfo(),
                    uri.getHost().toLowerCase(Locale.ROOT),
                    uri.getPort(),
                    path,
                    uri.getQuery(),
                    null
            );
        } catch (Exception e) {
            return null;
        }
    }
}