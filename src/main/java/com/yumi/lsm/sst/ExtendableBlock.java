package com.yumi.lsm.sst;

import com.yumi.lsm.Config;

public class ExtendableBlock extends Block {
    public ExtendableBlock(Config config) {
        super(config);
    }

    @Override
    protected boolean isFixedSize() {
        return false;
    }
}
