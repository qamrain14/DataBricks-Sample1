import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { ApiService } from './api.service';
import { PagedResult, ClassDto, SubjectDto, SectionDto, TimelineDto, ClassOfferingDto, OfferingFilterRequest } from '../models';

@Injectable({ providedIn: 'root' })
export class CurriculumService {
  constructor(private api: ApiService) {}

  // Classes
  getClasses(page = 1, pageSize = 10): Observable<PagedResult<ClassDto>> {
    return this.api.getPaged<ClassDto>('classes', page, pageSize);
  }

  getClass(id: number): Observable<ClassDto> {
    return this.api.getById<ClassDto>('classes', id);
  }

  createClass(data: Partial<ClassDto>): Observable<ClassDto> {
    return this.api.post<ClassDto>('classes', data);
  }

  updateClass(id: number, data: Partial<ClassDto>): Observable<ClassDto> {
    return this.api.put<ClassDto>('classes', id, data);
  }

  deleteClass(id: number): Observable<void> {
    return this.api.delete('classes', id);
  }

  // Subjects
  getSubjects(page = 1, pageSize = 10): Observable<PagedResult<SubjectDto>> {
    return this.api.getPaged<SubjectDto>('subjects', page, pageSize);
  }

  getSubject(id: number): Observable<SubjectDto> {
    return this.api.getById<SubjectDto>('subjects', id);
  }

  createSubject(data: Partial<SubjectDto>): Observable<SubjectDto> {
    return this.api.post<SubjectDto>('subjects', data);
  }

  updateSubject(id: number, data: Partial<SubjectDto>): Observable<SubjectDto> {
    return this.api.put<SubjectDto>('subjects', id, data);
  }

  deleteSubject(id: number): Observable<void> {
    return this.api.delete('subjects', id);
  }

  // Sections
  getSections(page = 1, pageSize = 10): Observable<PagedResult<SectionDto>> {
    return this.api.getPaged<SectionDto>('sections', page, pageSize);
  }

  getSection(id: number): Observable<SectionDto> {
    return this.api.getById<SectionDto>('sections', id);
  }

  createSection(data: Partial<SectionDto>): Observable<SectionDto> {
    return this.api.post<SectionDto>('sections', data);
  }

  updateSection(id: number, data: Partial<SectionDto>): Observable<SectionDto> {
    return this.api.put<SectionDto>('sections', id, data);
  }

  deleteSection(id: number): Observable<void> {
    return this.api.delete('sections', id);
  }

  // Timelines
  getTimelines(page = 1, pageSize = 10): Observable<PagedResult<TimelineDto>> {
    return this.api.getPaged<TimelineDto>('timelines', page, pageSize);
  }

  getTimeline(id: number): Observable<TimelineDto> {
    return this.api.getById<TimelineDto>('timelines', id);
  }

  createTimeline(data: Partial<TimelineDto>): Observable<TimelineDto> {
    return this.api.post<TimelineDto>('timelines', data);
  }

  updateTimeline(id: number, data: Partial<TimelineDto>): Observable<TimelineDto> {
    return this.api.put<TimelineDto>('timelines', id, data);
  }

  deleteTimeline(id: number): Observable<void> {
    return this.api.delete('timelines', id);
  }

  // Class Offerings
  getOfferings(filters?: OfferingFilterRequest): Observable<PagedResult<ClassOfferingDto>> {
    return this.api.get<PagedResult<ClassOfferingDto>>('classofferings', filters);
  }

  getOffering(id: number): Observable<ClassOfferingDto> {
    return this.api.getById<ClassOfferingDto>('classofferings', id);
  }

  createOffering(data: Partial<ClassOfferingDto>): Observable<ClassOfferingDto> {
    return this.api.post<ClassOfferingDto>('classofferings', data);
  }

  updateOffering(id: number, data: Partial<ClassOfferingDto>): Observable<ClassOfferingDto> {
    return this.api.put<ClassOfferingDto>('classofferings', id, data);
  }

  deleteOffering(id: number): Observable<void> {
    return this.api.delete('classofferings', id);
  }
}
