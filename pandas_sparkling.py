def myfunc(anotherfunc, *extraArgs):
      return anotherfunc(*extraArgs)
def pd_apply(df,func,*extraArgs):

    df = df.rdd.map(
                    lambda line:
                    myfunc(func,line,*extraArgs)
                    ).toDF()
    return df
