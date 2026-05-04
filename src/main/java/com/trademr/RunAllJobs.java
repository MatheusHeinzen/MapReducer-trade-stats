package com.trademr;

import com.trademr.common.JobPaths;
import com.trademr.common.MrLocal;
import com.trademr.job1.Q1BrazilCountDriver;
import com.trademr.job2.Q2TransactionsPerYearDriver;
import com.trademr.job3.Q3TransactionsPerCategoryDriver;
import com.trademr.job4.Q4TransactionsPerFlowDriver;
import com.trademr.job5.Q5BrazilAvgPriceDriver;
import com.trademr.job6.Q6Brazil2016MinMaxDriver;
import com.trademr.job7.Q7BrazilExportAvgDriver;
import com.trademr.job8.Q8MinMaxAmountPerYearCountryDriver;
import org.apache.hadoop.conf.Configuration;

/**
 * Executa os 8 jobs em sequência com o mesmo {@link Configuration} (modo local).
 * <p>
 * Argumentos: {@code [caminho_csv]}
 * <p>
 * IntelliJ: Main class {@code com.trademr.RunAllJobs}, working directory = raiz do módulo
 * {@code mapreduce-trade-stats}, Program arguments = vazio ou caminho do CSV.
 */
public final class RunAllJobs {

  private RunAllJobs() {}

  public static void main(String[] args) throws Exception {
    MrLocal.bootstrapWindowsBeforeHadoop();
    Configuration conf = new Configuration();
    MrLocal.apply(conf);

    String in = args.length > 0 ? args[0] : JobPaths.DEFAULT_INPUT;

    Step[] steps = {
        new Step("Q1 Brasil", (c) -> Q1BrazilCountDriver.run(pair(in, "out/q1_brazil_count"), c)),
        new Step("Q2 Por ano", (c) -> Q2TransactionsPerYearDriver.run(pair(in, "out/q2_per_year"), c)),
        new Step("Q3 Categoria", (c) -> Q3TransactionsPerCategoryDriver.run(pair(in, "out/q3_per_category"), c)),
        new Step("Q4 Flow", (c) -> Q4TransactionsPerFlowDriver.run(pair(in, "out/q4_per_flow"), c)),
        new Step("Q5 Média preço BR", (c) -> Q5BrazilAvgPriceDriver.run(pair(in, "out/q5_avg_price_brazil"), c)),
        new Step("Q6 Min/max preço BR 2016", (c) -> Q6Brazil2016MinMaxDriver.run(pair(in, "out/q6_minmax_price_brazil_2016"), c)),
        new Step("Q7 Média Export BR", (c) -> Q7BrazilExportAvgDriver.run(pair(in, "out/q7_avg_price_brazil_export"), c)),
        new Step("Q8 Min/max Amount", (c) -> Q8MinMaxAmountPerYearCountryDriver.run(pair(in, "out/q8_minmax_amount_year_country"), c)),
    };

    for (Step step : steps) {
      System.err.println(">>> " + step.name);
      int code = step.fn.run(conf);
      if (code != 0) {
        System.err.println("Falhou: " + step.name);
        System.exit(code);
      }
    }
    System.err.println("Todos os jobs concluíram OK.");
  }

  private static String[] pair(String input, String output) {
    return new String[] {input, output};
  }

  @FunctionalInterface
  private interface JobRunner {
    int run(Configuration conf) throws Exception;
  }

  private static final class Step {
    final String name;
    final JobRunner fn;

    Step(String name, JobRunner fn) {
      this.name = name;
      this.fn = fn;
    }
  }
}
