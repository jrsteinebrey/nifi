/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatSidenavModule } from '@angular/material/sidenav';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { Canvas } from './canvas.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/flow/flow.reducer';
import { ContextMenu } from '../../../../ui/common/context-menu/context-menu.component';
import { CdkContextMenuTrigger } from '@angular/cdk/menu';
import { selectBreadcrumbs } from '../../state/flow/flow.selectors';
import { BreadcrumbEntity } from '../../state/shared';
import { MockComponent } from 'ng-mocks';
import { GraphControls } from './graph-controls/graph-controls.component';
import { HeaderComponent } from './header/header.component';
import { FooterComponent } from './footer/footer.component';
import { canvasFeatureKey } from '../../state';
import { flowFeatureKey } from '../../state/flow';
import { FlowAnalysisDrawerComponent } from './header/flow-analysis-drawer/flow-analysis-drawer.component';
import { CanvasActionsService } from '../../service/canvas-actions.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('Canvas', () => {
    let component: Canvas;
    let fixture: ComponentFixture<Canvas>;

    beforeEach(() => {
        const breadcrumbEntity: BreadcrumbEntity = {
            id: '',
            permissions: {
                canRead: false,
                canWrite: false
            },
            versionedFlowState: '',
            breadcrumb: {
                id: '',
                name: ''
            }
        };

        TestBed.configureTestingModule({
            declarations: [Canvas],
            imports: [
                CdkContextMenuTrigger,
                ContextMenu,
                MatSidenavModule,
                NoopAnimationsModule,
                MatSidenavModule,
                HttpClientTestingModule,
                MockComponent(GraphControls),
                MockComponent(HeaderComponent),
                MockComponent(FooterComponent),
                FlowAnalysisDrawerComponent
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialState
                        }
                    },
                    selectors: [
                        {
                            selector: selectBreadcrumbs,
                            value: breadcrumbEntity
                        }
                    ]
                }),
                CanvasActionsService
            ]
        });
        fixture = TestBed.createComponent(Canvas);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
